from executor_service.database import db_session as _db_session


async def db_session():
    async with _db_session() as session:
        yield session
