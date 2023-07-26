from fastapi import APIRouter, Depends
from pydantic import BaseModel, validator

from sqlalchemy import select
from sqlalchemy.orm import load_only
from sqlalchemy.ext.asyncio import AsyncSession

from executor_service.models.queries import QueryExecution
from executor_service.services.crypto import encrypt, decrypt
from executor_service.dependencies import db_session, get_user
from executor_service.settings import settings

router = APIRouter(prefix='/keys', tags=['keys'])


class KeyIn(BaseModel):
    old_key: str

    @validator('old_key')
    def key_must_be_hex(cls, v):
        bytes.fromhex(v)
        return v


@router.post('/rotate')
async def rotate(key_in: KeyIn, session: AsyncSession = Depends(db_session), _=Depends(get_user)):
    query_executions = await session.execute(
        select(QueryExecution).with_for_update(nowait=True)
        .options(load_only(QueryExecution.db))
    )
    query_executions = query_executions.scalars().all()

    for query_exec in query_executions:
        decrypted_conn_string = decrypt(key_in.old_key, query_exec.db)
        if decrypted_conn_string:
            encrypted_conn_string = encrypt(settings.encryption_key, decrypted_conn_string)
            query_exec.db = encrypted_conn_string
            session.add(query_exec)
