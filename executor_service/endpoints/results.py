# type: ignore[no-untyped-def]
import asyncio
from typing import Dict, List, Union

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from executor_service.schemas.queries import QueryIn
from executor_service.services.executor import execute_query, get_query_result, terminate_query
from executor_service.dependencies import db_session, get_user
from executor_service.models.queries import QueryExecution, QueryDestination


router = APIRouter(
    prefix="/results",
    tags=["results"],
    responses={404: {"description": "Not found"}},
)

def available_to_user(reult: QueryDestination, user: dict):
    if user.get('is_superuser'):
        return True
    return reult.identity_id == user['identity_id']


@router.get("/{guid}", response_model=Dict)
async def get_result(guid: str, session = Depends(db_session), user = Depends(get_user)):
    result = await session.execute(
        select(QueryDestination)
        .filter(QueryDestination.guid == guid)
    )
    result = result.scalars().first()
    if result is None:
        raise HTTPException(status_code=404)

    if not available_to_user(result, user):
        raise HTTPException(status_code=401)

    return result



@router.get('/{guid}/download', response_model=Dict)
async def download_result(guid: str, session = Depends(db_session), user = Depends(get_user)):
    pass