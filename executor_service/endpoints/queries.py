# type: ignore[no-untyped-def]
from typing import Dict, List, Union

from fastapi import APIRouter

from executor_service.services.executor import ExecutorService

router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=Union[List[Dict], Dict])
async def upload_item(query: str):
    result = await ExecutorService().execute_query(query)
    return result
