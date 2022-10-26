from pydantic import BaseModel


class QueryIn(BaseModel):
    query: str
    table: str


class QueryPidIn(BaseModel):
    query_pid: int
    table: str
