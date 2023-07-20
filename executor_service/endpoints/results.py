# type: ignore[no-untyped-def]
import io
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
import pandas as pd
from starlette.responses import StreamingResponse

from executor_service.dependencies import db_session, get_user
from executor_service.models.queries import QueryDestination

router = APIRouter(
    prefix="/results",
    tags=["results"],
    responses={404: {"description": "Not found"}}
)


def available_to_user(reult: QueryDestination, user: dict):
    if user.get('is_superuser'):
        return True
    return reult.identity_id == user['identity_id']


@router.get("/{id}", response_model=Dict)
async def get_result(id: int, session=Depends(db_session), user=Depends(get_user)):
    result = await session.execute(
        select(QueryDestination)
        .filter(QueryDestination.id == id)
    )
    result = result.scalars().first()
    if result is None:
        raise HTTPException(status_code=404)

    if not available_to_user(result, user):
        raise HTTPException(status_code=401)

    return result


@router.get('/{id}/download')
async def download_result(id: int, session=Depends(db_session), user=Depends(get_user)):
    result = await session.execute(
        select(QueryDestination)
        .filter(QueryDestination.id == id)
    )
    result = result.scalars().first()
    if result is None:
        raise HTTPException(status_code=404)

    if not available_to_user(result, user):
        raise HTTPException(status_code=401)

    df = pd.DataFrame({
        "dest_type": [result.dest_type],
        "status": [result.status],
        "id": [result.id],
        "access_creds": [result.access_creds],
        "query_id": [result.query_id],
        "path": [result.path],
        "error_description": [result.error_description],
        "finished_at": [result.finished_at]
    })
    stream = io.StringIO()
    df.to_csv(stream, index=False)

    response = StreamingResponse(
        iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response
