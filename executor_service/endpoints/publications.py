from fastapi import APIRouter

from executor_service.database import clickhouse_client

router = APIRouter(
    prefix="/publications",
    tags=["publications"],

)


MAX_LIMIT = 1000


@router.get('/', response_model=bool)
async def publish_exist(publish_name: str):
    return clickhouse_client.exist(publish_name)
