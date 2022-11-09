# type: ignore[no-untyped-def]
import asyncio
import json
from typing import Dict, List, Union

from fastapi import APIRouter

from executor_service.schemas.queries import QueryIn, QueryPidIn
from executor_service.services.executor import ExecutorService
from executor_service.mq import create_channel

from settings import settings

router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


async def execute_query(guid, sql_query, db):
    try:
        query_pid, result_data = await ExecutorService().execute_query(sql_query, db)
        result = 'done'
    except Exception as e:
        result = 'error'

    async with create_channel() as channel:
        await channel.basic_publish(
            exchange=settings.exchange_execute,
            routing_key='result',
            body=json.dumps({
                'guid': guid,
                'status': result,
            })
        )


@router.post("/", response_model=Union[List[Dict], Dict])
async def execute(query_data: QueryIn):
    task = execute_query(
        guid=query_data.guid,
        sql_query=query_data.query,
        db=query_data.db
    )
    asyncio.create_task(task)
    return {}


@router.get("/{query_pid}/", response_model=List[Dict])
async def get_result(query_data: QueryPidIn):
    result = await ExecutorService().get_query_result(query_data.query_pid, query_data.table)
    return result


@router.delete("/{query_pid}/", response_model=str)
async def terminate(query_data: QueryPidIn):
    result = await ExecutorService().terminate_query(query_data.query_pid, query_data.table)
    return result
