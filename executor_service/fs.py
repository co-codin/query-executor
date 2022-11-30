import asyncio
import httpx
from contextlib import asynccontextmanager
from concurrent.futures.thread import ThreadPoolExecutor
from minio import Minio
from executor_service._minio import build_minio_request
from executor_service.settings import settings


_EXECUTOR = ThreadPoolExecutor(max_workers=settings.thread_pool_size)


class FsClient:
    def __init__(self, client):
        self.client = client

    async def create_user(self, access_key, secret_key):
        return await self._request('PUT', 'add-user', query_params={'accessKey': access_key}, payload={
            'secretKey': secret_key,
            'status': 'enabled'
        }, encrypt_payload=True)

    async def create_policy(self, name, statements):
        return await self._request('PUT', 'add-canned-policy', query_params={'name': name}, payload={
            "Version": "2012-10-17",
            "Statement": statements
        })

    async def attach_policy(self, access_key, policy_name):
        return await self._request('PUT', 'set-user-or-group-policy', query_params={
            'policyName': policy_name,
            'userOrGroup': access_key,
            'isGroup': 'false'
        })

    async def ensure_bucket(self, bucket_name):
        loop = asyncio.get_running_loop()

        def _do():
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)

        await loop.run_in_executor(_EXECUTOR, _do)

    async def upload_file(self, bucket_name, name, path):
        loop = asyncio.get_running_loop()

        def _do():
            self.client.fput_object(bucket_name, name, path)

        await loop.run_in_executor(_EXECUTOR, _do)

    async def _request(
        self, method: str, action: str, query_params: dict,
        payload: dict = None, encrypt_payload: bool = False
    ):
        url, headers, payload = build_minio_request(
            method, action,
            query_params=query_params,
            payload=payload,
            encrypt_payload=encrypt_payload
        )

        async with httpx.AsyncClient() as requests:
            response = await requests.request(method, url, headers=headers, content=payload)
            data = response.text

        return data


@asynccontextmanager
async def fs_client() -> FsClient:
    client = Minio(
        settings.minio_host,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=False
    )
    yield FsClient(client)
