# type: ignore[no-untyped-def]
from typing import Dict, List, Union

from fastapi import APIRouter

from executor_service.schemas.queries import QueryIn, QueryPidIn
from executor_service.services.executor import ExecutorService

router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=Union[List[Dict], Dict])
async def execute(query_data: QueryIn):
    result = await ExecutorService().execute_query(query_data.query, query_data.table)
    return result


@router.get("/{query_pid}/", response_model=Union[List[Dict], Dict])
async def get_result(query_data: QueryPidIn):
    result = await ExecutorService().get_query_result(query_data.query_pid, query_data.table)
    return result


@router.delete("/{query_pid}/", response_model=str)
async def terminate(query_data: QueryPidIn):
    result = await ExecutorService().terminate_query(query_data.query_pid, query_data.table)
    return result
