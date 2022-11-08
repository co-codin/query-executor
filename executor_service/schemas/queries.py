from typing import Dict, List

from pydantic import BaseModel


class QueryIn(BaseModel):
    query: str
    table: str


class QueryOut(BaseModel):
    pid_info: str
    result: List[Dict]


class QueryPidIn(BaseModel):
    query_pid: int
    table: str
