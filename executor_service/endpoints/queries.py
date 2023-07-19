# type: ignore[no-untyped-def]
import asyncio
from typing import Dict, List, Union

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from executor_service.schemas.queries import QueryIn
from executor_service.services.executor import execute_query, get_query_result, terminate_query
from executor_service.dependencies import db_session, get_user
from executor_service.models.queries import QueryExecution, QueryDestination


router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)


MAX_LIMIT = 1000


def available_to_user(query: QueryExecution, user: dict):
    if user.get('is_superuser'):
        return True
    return query.identity_id == user['identity_id']


@router.post("/", response_model=Union[List[Dict], Dict])
async def execute(query_data: QueryIn, session = Depends(db_session)):
    query = QueryExecution(
        guid=query_data.guid,
        query=query_data.query,
        db=query_data.db,
        identity_id=query_data.identity_id,
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
async def get_query(query_id: int, session = Depends(db_session), user = Depends(get_user)):
    query = await session.execute(
        select(QueryExecution).options(selectinload(QueryExecution.results)).where(QueryExecution.id == query_id)
    )
    query = query.scalars().first()
    if query is None:
        raise HTTPException(status_code=404)

    if not available_to_user(query, user):
        raise HTTPException(status_code=401)

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
                     session = Depends(db_session),
                     user = Depends(get_user)):
    query = await session.execute(
        select(QueryExecution).options(selectinload(QueryExecution.results)).where(QueryExecution.id == query_id)
    )
    query = query.scalars().first()
    if query is None:
        raise HTTPException(status_code=404)

    if not available_to_user(query, user):
        raise HTTPException(status_code=401)

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


@router.delete('/{guid}/delete_by_guid')
async def delete_result(guid: str, session = Depends(db_session), user = Depends(get_user)):
    query = await session.execute(
        select(QueryExecution)
        .filter(QueryExecution.guid == guid)
    )
    query = query.scalars().first()
    if query is None:
        raise HTTPException(status_code=404)

    if available_to_user(result, user):
        await session.execute(
            delete(QueryDestination)
            .where(QueryDestination.query_id == query.id)
        )
        await session.execute(
            delete(QueryExecution)
            .where(QueryExecution.id == query.id)
        )
        await session.commit()
    else:
        await session.execute(
            delete(QueryExecution)
            .where(QueryExecution.id == query.id)
            .where(QueryExecution.identity_id == user['identity_id'])
        )
        await session.commit()