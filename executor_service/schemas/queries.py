from typing import Dict, List
from enum import Enum
from pydantic import BaseModel


class DestinationType(Enum):
    table = 'table'
    csv = 'file'


class QueryIn(BaseModel):
    guid: str
    query: str
    db: str
    result_destinations: List[DestinationType]


class QueryResultIn(BaseModel):
    limit: int
    offset: int


class QueryOut(BaseModel):
    pid_info: str
    result: List[Dict]


class QueryPidIn(BaseModel):
    query_pid: int
    table: str
