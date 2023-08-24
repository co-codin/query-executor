from contextlib import asynccontextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base

from executor_service.services.clickhouse import ClickhouseService
from executor_service.settings import settings


engine = create_async_engine(
    settings.db_connection_string,
    echo=settings.debug,
    pool_pre_ping=True
)

clickhouse_client = ClickhouseService().connect()

async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

Base = declarative_base()


@asynccontextmanager
async def db_session():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
