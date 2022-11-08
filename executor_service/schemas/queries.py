from pydantic import BaseModel


class QueryIn(BaseModel):
    guid: str
    query: str
    db: str


class QueryPidIn(BaseModel):
    query_pid: int
    table: str
