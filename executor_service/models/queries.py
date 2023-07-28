from uuid import uuid4
from enum import Enum
from datetime import datetime
from sqlalchemy import Column, DateTime, BigInteger, String, Text, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from executor_service.database import Base


class QueryStatus(Enum):
    CREATED = 'created'
    DONE = 'done'
    RUNNING = 'running'
    CANCELLED = 'cancelled'
    ERROR = 'error'


class QueryDestinationStatus(Enum):
    DECLARED = 'declared'
    UPLOADED = 'uploaded'
    ERROR = 'error'
    DELETED = 'deleted'


class QueryExecution(Base):
    __tablename__ = 'queries'

    id = Column(BigInteger, primary_key=True)
    guid = Column(String(36), nullable=False, default=lambda: str(uuid4()), unique=True, index=True)
    db = Column(String(728), nullable=False)
    query = Column(Text, nullable=False)
    identity_id = Column(String(36))
    status = Column(String(36), default=QueryStatus.CREATED.value)
    error_description = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, server_onupdate=func.now())

    results = relationship('QueryDestination')


class QueryDestination(Base):
    __tablename__ = 'results'

    id = Column(BigInteger, primary_key=True)
    query_id = Column(BigInteger, ForeignKey('queries.id'))
    dest_type = Column(String(36), nullable=False)
    path = Column(Text, nullable=True)
    status = Column(String(36), default=QueryDestinationStatus.DECLARED.value)
    error_description = Column(Text, nullable=True)
    access_creds = Column(Text)
    finished_at = Column(DateTime)
