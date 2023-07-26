import os
import base64
import logging

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


logger = logging.getLogger(__name__)


def encrypt(key: str, data: str) -> str:
    key = bytes.fromhex(key)
    data = data.encode()
    nonce = os.urandom(12)
    aad = os.urandom(16)
    encrypted = nonce + AESGCM(key).encrypt(nonce, data, aad) + aad
    return base64.b64encode(encrypted).hex()


def decrypt(key: str, data: str) -> str | None:
    key = bytes.fromhex(key)
    data = bytes.fromhex(data)
    data = base64.b64decode(data)
    nonce = data[:12]
    aad = data[-16:]
    encrypted = data[12:-16]
    try:
        decrypted = AESGCM(key).decrypt(nonce, encrypted, aad)
        decrypted = decrypted.decode('utf-8')
    except Exception as err:
        logger.warning(str(err))
        decrypted = None
    return decrypted
