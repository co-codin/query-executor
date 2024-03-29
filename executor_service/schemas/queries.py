from enum import Enum
from pydantic import BaseModel


class DestinationType(Enum):
    table = 'table'
    csv = 'file'


class QueryIn(BaseModel):
    guid: str
    run_guid: str
    query: str
    result_destinations: list[DestinationType]
    identity_id: str
    conn_string: str

class QueryErrorIn(BaseModel):
    guid: str
    run_guid: str
    query: str
    error_description: str
    identity_id: str
    conn_string: str

class QueryResultIn(BaseModel):
    limit: int
    offset: int


class QueryOut(BaseModel):
    pid_info: str
    result: list[dict]


class QueryDeleteIn(BaseModel):
    guids: list[str] = []


class QueryPublishIn(BaseModel):
    guid: str
    publish_name: str
    identity_id: str
