from typing import Dict, List
from enum import Enum
from pydantic import BaseModel


class DestinationType(Enum):
    table = 'table'
    csv = 'csv'


class QueryIn(BaseModel):
    guid: str
    query: str
    db: str
    result_dest: DestinationType


class QueryOut(BaseModel):
    pid_info: str
    result: List[Dict]


class QueryPidIn(BaseModel):
    query_pid: int
    table: str
