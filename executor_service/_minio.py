import json
import secrets
import hashlib
import urllib.parse
from io import BytesIO
from datetime import datetime, timezone

from argon2.low_level import hash_secret_raw, Type as ArgonType
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from minio.signer import sign_v4_s3
from minio.credentials import Credentials

from settings import settings


__all__ = [
    'build_minio_request',
]


# размер блока определенный протоколом minio,
# этого достаточно для подавляющего большинства запросов
# поэтому вся информация будет предствавлена одним блоком
BLOCK_SIZE = 1 << 14


def next_nonce(nonce: bytes = None) -> bytes:
    # в протоколе minio nonce представлен в виде 8 случайных байт
    # и порядковым номером блока в little endian
    if nonce is None:
        nonce = secrets.token_bytes(8) + b'\x00' * 4
        return nonce

    buf: bytes = nonce[-4:]
    idx: int = int.from_bytes(buf, byteorder='little') + 1
    buf = nonce[:-4] + idx.to_bytes(4, byteorder='little')
    return buf


def _encrypt(cipher: AESGCM, data: bytes, associated_data: bytes = None) -> (bytes, bytes):
    nonce = initial_nonce = next_nonce()
    # в протоколе дополнительно шифруются добавочные данные
    associated_data = b'\x00' + cipher.encrypt(nonce, b'', associated_data)

    # тут должен быть цикл записи шифрованых блоков, но для наших целей это необязательно
    if (len(data)) > BLOCK_SIZE:
        raise ValueError('Message is too long')

    # закрывающий блок, пишется как есть
    # добавочные данные последнего блока имеют другой префикс
    associated_data = b'\x80' + associated_data[1:]
    nonce = next_nonce(nonce)
    block = cipher.encrypt(nonce, data, associated_data)

    return initial_nonce, block


def encrypt(password: str, string: str):
    data: bytes = string.encode('ascii')
    secret: bytes = password.encode('ascii')
    salt:bytes = secrets.token_bytes(32)

    # настройки хеширования ключа взяты из minio, используем только Argon2 ID
    key = hash_secret_raw(
        secret,
        salt,
        time_cost=1,
        memory_cost=64 * 1024,
        parallelism=4,
        hash_len=32,
        type=ArgonType.ID
    )
    aesgcm = AESGCM(key)
    nonce,  edata = _encrypt(aesgcm, data)

    buf = BytesIO()
    buf.write(salt)
    # идентификатор типа сайфера и хеша ключа
    buf.write((0).to_bytes(1, byteorder='big'))
    buf.write(nonce[:8])
    buf.write(edata)

    return buf.getvalue()


def _to_utc(value):
    """Convert to UTC time if value is not naive."""
    return (
        value.astimezone(timezone.utc).replace(tzinfo=None)
        if value.tzinfo else value
    )


def to_amz_date(date):
    """Format datetime into AMZ date formatted string."""
    return _to_utc(date).strftime("%Y%m%dT%H%M%SZ")


def sha256_hash(data):
    data = data or b""
    hasher = hashlib.sha256()
    hasher.update(data.encode() if isinstance(data, str) else data)
    sha256sum = hasher.hexdigest()
    return sha256sum.decode() if isinstance(sha256sum, bytes) else sha256sum


def _build_headers(host, headers, body):
    headers = headers or {}
    headers["Host"] = host
    headers["User-Agent"] = 'MinIO (linux; amd64) madmin-go/2.0.0'

    if body:
        headers["Content-Length"] = str(len(body))
    headers["X-Amz-Content-Sha256"] = sha256_hash(body)
    date = datetime.utcnow().replace(tzinfo=timezone.utc)
    headers["X-Amz-Date"] = to_amz_date(date)
    return headers, date


def build_minio_request(
        method: str, action: str, query_params: dict = None,
        payload: dict = None, encrypt_payload: bool=False):
    method = method.upper()
    if payload is not None:
        payload = json.dumps(payload)
    if query_params is not None:
        query_params = urllib.parse.urlencode(query_params)

    url = f'http://{settings.minio_host}/minio/admin/v3/{action}?{query_params}'
    creds = Credentials(
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
    region = ''
    if encrypt_payload:
        payload = encrypt(creds.secret_key, payload)
    headers, date = _build_headers(settings.minio_host, {}, payload)
    headers = sign_v4_s3(
        method,
        urllib.parse.urlsplit(url),
        region,
        headers,
        creds,
        headers.get("X-Amz-Content-Sha256"),
        date,
    )

    return url, headers, payload
