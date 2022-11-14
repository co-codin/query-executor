from uuid import uuid4
from enum import Enum
from datetime import datetime
from sqlalchemy import Column, DateTime, BigInteger, String, Text, Boolean
from sqlalchemy.sql import func

from executor_service.database import Base


__all__ = (
    'Query',
)


class QueryStatus(Enum):
    CREATED = 'created'
    DONE = 'done'
    ERROR = 'error'


class Query(Base):
    __tablename__ = 'queries'

    id = Column(BigInteger, primary_key=True)
    guid = Column(String(36), nullable=False, default=lambda: str(uuid4()), unique=True, index=True)
    db = Column(String(36), nullable=False)
    query = Column(Text, nullable=False)
    status = Column(String(36), default=QueryStatus.CREATED.value)
    error_description = Column(Text, nullable=True)

    results_file = Column(Text, nullable=True)
    results_table = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, server_onupdate=func.now())
