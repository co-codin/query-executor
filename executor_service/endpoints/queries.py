# type: ignore[no-untyped-def]
from typing import Dict, List

from fastapi import APIRouter

from executor_service.schemas.queries import QueryIn, QueryOut, QueryPidIn
from executor_service.services.executor import ExecutorService

router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=QueryOut)
async def execute(query_data: QueryIn):
    query_pid, result_data = await ExecutorService().execute_query(query_data.query, query_data.table)
    return QueryOut(pid_info=query_pid, result=result_data)


@router.get("/{query_pid}/", response_model=List[Dict])
async def get_result(query_data: QueryPidIn):
    result = await ExecutorService().get_query_result(query_data.query_pid, query_data.table)
    return result


@router.delete("/{query_pid}/", response_model=str)
async def terminate(query_data: QueryPidIn):
    result = await ExecutorService().terminate_query(query_data.query_pid, query_data.table)
    return result
