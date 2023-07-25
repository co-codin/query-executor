# type: ignore[no-untyped-def]
import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from executor_service.schemas.queries import QueryIn
from executor_service.services.executor import execute_query, get_query_result, terminate_query, send_notification
from executor_service.dependencies import db_session, get_user
from executor_service.models.queries import QueryExecution, QueryDestination
from executor_service.services.crypto import encrypt
from executor_service.settings import settings


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


@router.post("/", response_model=dict[str, int | str])
async def execute(query_data: QueryIn, session=Depends(db_session)):
    query = QueryExecution(
        guid=query_data.run_guid,
        query=query_data.query,
        db=encrypt(settings.encryption_key, query_data.conn_string),
        identity_id=query_data.identity_id,
    )
    session.add(query)

    for dest_type in query_data.result_destinations:
        dest = QueryDestination(dest_type=dest_type.value)
        query.results.append(dest)
    await session.commit()

    asyncio.create_task(execute_query(query.id, query_data.conn_string))
    return {
        'id': query.id,
        'guid': query.guid,
    }


@router.get("/{query_guid}", response_model=dict)
async def get_query(query_guid: str, session=Depends(db_session), user=Depends(get_user)):
    query = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.guid == query_guid)
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
        }
            for dest in query.results
        ],
    }


@router.get("/{query_guid}/results", response_model=list[dict])
async def get_result(query_guid: str,
                     limit: int = Query(default=MAX_LIMIT, gt=0, lt=MAX_LIMIT),
                     offset: int = Query(default=0, ge=0),
                     session=Depends(db_session),
                     user=Depends(get_user)):
    query = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.guid == query_guid)
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


@router.delete("/{query_guid}")
async def terminate(query_guid: str):
    query = await terminate_query(query_guid)
    await send_notification(query)

