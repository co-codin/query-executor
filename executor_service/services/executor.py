from typing import Dict, List, Union

import asyncpg
from asyncpg import Connection

from settings import settings

from executor_service.errors import QueryNotFoundError

QUERIES = {}


class ExecutorService:

    async def execute_query(self, query: str, db: str) -> Union[List[Dict], Dict]:
        conn_string = settings.db_sources[db]
        conn = await asyncpg.connect(conn_string)
        query_pid_result = await conn.execute("SELECT pg_backend_pid();")
        query_pid = query_pid_result.split()[-1]
        if 'select' in query.lower():
            data = await self._process_select(conn, query)  # 'SELECT * FROM dv_raw.case_hub LIMIT 5;'
        elif 'delete' in query.lower() or 'update' in query.lower():
            async with conn.transaction():
                await conn.execute(query)
                data = {"result": "success"}
        global QUERIES
        QUERIES[f"{db}_{query_pid}"] = data
        return data

    async def get_query_result(self, query_pid: int, table: str) -> Union[List[Dict], Dict]:
        try:
            return QUERIES[f"{table}_{query_pid}"]
        except KeyError:
            raise QueryNotFoundError(query_pid)

    async def terminate_query(self, query_pid: int, table: str) -> str:
        try:
            conn_string = f"{settings.db_driver}://{settings.db_user}:{settings.db_password}@" \
                          f"{settings.db_host}:{settings.db_port}/{table}"
            conn = await asyncpg.connect(conn_string)
            result = await conn.execute(f"SELECT pg_cancel_pid({query_pid});")  # may use pg_terminate_pid
            return result
        except KeyError:
            raise QueryNotFoundError(query_pid)

    async def _process_select(self, conn: Connection, query: str) -> List[Dict]:
        rows = await conn.fetch(query)
        return [dict(row) for row in rows]
