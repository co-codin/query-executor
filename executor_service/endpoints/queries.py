# type: ignore[no-untyped-def]
import asyncio
import json
from typing import Dict, List, Union

from fastapi import APIRouter, Depends

from executor_service.schemas.queries import QueryIn, QueryPidIn
from executor_service.services.executor import execute_query
from executor_service.dependencies import db_session
from executor_service.models.queries import Query
from executor_service.mq import create_channel

from settings import settings

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
        db=query_data.db
    )
    session.add(query)
    await session.commit()
    #session.refresh(query)

    await execute_query(query)
    #asyncio.create_task(ExecutorService().execute_query(query))
    return {}


@router.get("/{query_pid}/", response_model=List[Dict])
async def get_result(query_data: QueryPidIn):
    result = await ExecutorService().get_query_result(query_data.query_pid, query_data.table)
    return result


@router.delete("/{query_pid}/", response_model=str)
async def terminate(query_data: QueryPidIn):
    result = await ExecutorService().terminate_query(query_data.query_pid, query_data.table)
    return result
