import asyncio
from contextlib import asynccontextmanager
from concurrent.futures.thread import ThreadPoolExecutor
from minio import Minio
from settings import settings


_EXECUTOR = ThreadPoolExecutor(max_workers=settings.thread_pool_size)


class FsClient:
    def __init__(self, client):
        self.client = client

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


@asynccontextmanager
async def fs_client() -> FsClient:
    client = Minio(
        settings.minio_host,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=False
    )
    yield FsClient(client)
