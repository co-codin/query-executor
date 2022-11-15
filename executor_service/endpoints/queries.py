# type: ignore[no-untyped-def]
import asyncio
from typing import Dict, List, Union

from fastapi import APIRouter, Depends

from executor_service.schemas.queries import QueryIn, QueryPidIn
from executor_service.services.executor import execute_query
from executor_service.dependencies import db_session
from executor_service.models.queries import Query, QueryDestination


router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


@router.post("/", response_model=Union[List[Dict], Dict])
async def execute(query_data: QueryIn, session = Depends(db_session)):
    query = Query(
        guid=query_data.guid,
        query=query_data.query,
        db=query_data.db,

    )
    session.add(query)

    for dest_type in query_data.result_destinations:
        dest = QueryDestination(dest_type=dest_type.value)
        query.results.append(dest)
    await session.commit()

    asyncio.create_task(execute_query(query.id))
    return {}


@router.get("/{query_pid}/", response_model=List[Dict])
async def get_result(query_data: QueryPidIn):
    result = await ExecutorService().get_query_result(query_data.query_pid, query_data.table)
    return result


@router.delete("/{query_pid}/", response_model=str)
async def terminate(query_data: QueryPidIn):
    result = await ExecutorService().terminate_query(query_data.query_pid, query_data.table)
    return result
