from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer

from executor_service.mq import PikaChannel, create_channel
from executor_service.database.sqlalchemy import db_session as _db_session
from executor_service.auth import decode_jwt


bearer = HTTPBearer()


async def db_session():
    async with _db_session() as session:
        yield session


async def get_user(token=Depends(bearer)) -> dict:
    try:
        return await decode_jwt(token.credentials)
    except Exception:
        raise HTTPException(status_code=401)


async def mq_channel() -> PikaChannel:
    async with create_channel() as channel:
        yield channel
