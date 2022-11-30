# type: ignore[no-untyped-def]
import asyncio
from typing import Dict, List, Union

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from executor_service.schemas.queries import QueryIn, QueryPidIn
from executor_service.services.executor import execute_query, get_query_result, terminate_query
from executor_service.dependencies import db_session
from executor_service.models.queries import QueryExecution, QueryDestination


router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


MAX_LIMIT = 1000


@router.post("/", response_model=Union[List[Dict], Dict])
async def execute(query_data: QueryIn, session = Depends(db_session)):
    query = QueryExecution(
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
    return {
        'id': query.id,
        'guid': query.guid,
    }


@router.get("/{query_id}", response_model=Dict)
async def get_query(query_id: int, session = Depends(db_session)):
    query = await session.execute(
        select(QueryExecution).options(selectinload(QueryExecution.results)).where(QueryExecution.id == query_id)
    )
    query = query.scalars().first()
    if query is None:
        raise HTTPException(status_code=404)

    return {
        'status': query.status,
        'error': query.error_description,
        'result_destinations': [{
            'type': dest.dest_type,
            'status': dest.status,
            'error': dest.error_description,
            'path': dest.path,
            'creds': dest.access_creds,
        } for dest in query.results],
    }


@router.get("/{query_id}/results", response_model=List[Dict])
async def get_result(query_id: int,
                     limit: int = Query(default=None, gt=0, lt=MAX_LIMIT),
                     offset: int = Query(default=None, ge=0),
                     session = Depends(db_session)):
    query = await session.execute(
        select(QueryExecution).options(selectinload(QueryExecution.results)).where(QueryExecution.id == query_id)
    )
    query = query.scalars().first()
    if query is None:
        raise HTTPException(status_code=404)

    results = {
        dest.dest_type: dest for dest in query.results
    }
    if 'table' not in results:
        raise HTTPException(status_code=422, detail='Query does not have results stored in table')

    rows = await get_query_result(results['table'].path, limit, offset)
    return rows


@router.delete("/{query_id}")
async def terminate(query_id: int):
    await terminate_query(query_id)
    return {}
