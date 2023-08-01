# type: ignore[no-untyped-def]
import asyncio
import io
import pandas as pd

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from sqlalchemy import select, update
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio.session import AsyncSession

from executor_service.schemas.queries import QueryIn, QueryDeleteIn, QueryPublishIn
from executor_service.services.executor import (
    execute_query, get_query_result, terminate_query, send_notification, delete_query_execs
)
from executor_service.dependencies import db_session, get_user
from executor_service.models.queries import QueryExecution, QueryDestination, QueryDestinationStatus
from executor_service.services.crypto import encrypt
from executor_service.settings import settings
from executor_service.services.clickhouse import ClickhouseService

from datetime import datetime


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


async def select_query_exec(guid: str, user: dict, session: AsyncSession) -> QueryExecution:
    query = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.guid == guid)
    )
    query = query.scalars().first()
    if query is None:
        raise HTTPException(status_code=404)

    if not available_to_user(query, user):
        raise HTTPException(status_code=401)
    return query


async def select_query_execs(guids: list[str], user: dict, session: AsyncSession) -> list[QueryExecution]:
    queries = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.guid.in_(guids))
    )
    queries = queries.scalars().all()

    for query in queries:
        if not available_to_user(query, user):
            raise HTTPException(status_code=401)
    return queries


async def select_query_result(guid: str, limit: int, offset: int, user: dict, session: AsyncSession) -> list[dict]:
    query = await select_query_exec(guid, user, session)

    results = {
        dest.dest_type: dest for dest in query.results
    }
    if 'table' not in results:
        raise HTTPException(status_code=422, detail='Query does not have results stored in table')

    rows = await get_query_result(results['table'].path, limit, offset)
    return rows


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


@router.delete("/{guid}")
async def terminate(guid: str):
    query = await terminate_query(guid)
    await send_notification(query)


@router.get("/{guid}", response_model=dict)
async def get_query(guid: str, session=Depends(db_session), user=Depends(get_user)):
    query = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.guid == guid)
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


@router.get("/{guid}/results", response_model=list[dict])
async def get_result(guid: str,
                     limit: int = Query(default=MAX_LIMIT-1, gt=0, lt=MAX_LIMIT),
                     offset: int = Query(default=0, ge=0),
                     session=Depends(db_session),
                     user=Depends(get_user)):
    return await select_query_result(guid, limit, offset, user, session)


@router.get('/{guid}/download')
async def download_result(guid: str, session=Depends(db_session), user=Depends(get_user)):
    rows = await select_query_result(guid, MAX_LIMIT-1, 0, user, session)

    df = pd.DataFrame(rows)
    stream = io.StringIO()
    df.to_csv(stream, index=False)

    response = StreamingResponse(
        iter([stream.getvalue()]),
        media_type='text/csv',
        headers={
            'Content-Disposition': 'attachment; filename=result.csv',
            'Access-Control-Expose-Headers': 'Content-Disposition'
        }
    )
    return response


@router.post('/{guid}/publish')
async def publish_result(guid: str, 
                        limit: int = Query(default=MAX_LIMIT-1, gt=0, lt=MAX_LIMIT),
                        offset: int = Query(default=0, ge=0), 
                        session=Depends(db_session), 
                        user=Depends(get_user)
                        ):
    rows = await select_query_result(guid, limit, offset, user, session)

    df = pd.DataFrame(rows)
    
    clickhouseService = ClickhouseService()
    clickhouseService.connect()

    schema = ','.join([f'{col} String' for col in df.columns])
 
    clickhouseService.createPublishTable(guid, schema)

    try:
        clickhouseService.execute(f'INSERT INTO publish_{guid} ({",".join(df.columns)}) VALUES', rows)
        return JSONResponse(
            status_code=200,
            content={"message": "success"},
        )
    except:
        raise HTTPException(status_code=400)





@router.post('/delete-results')
async def delete_results(query_delete_in: QueryDeleteIn, session=Depends(db_session), user=Depends(get_user)):
    queries = await select_query_execs(query_delete_in.guids, user, session)
    if not queries:
        return
    results = [{dest.dest_type: dest for dest in query.results} for query in queries]
    try:
        paths = [res['table'].path for res in results]
    except KeyError:
        raise HTTPException(status_code=422, detail='Query does not have results stored in table')
    else:
        await delete_query_execs(paths)

    query_ids = [query.id for query in queries]
    await session.execute(
        update(QueryDestination)
        .where(QueryDestination.query_id.in_(query_ids))
        .values(status=QueryDestinationStatus.DELETED.value)
    )
