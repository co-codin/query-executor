from typing import Dict, List, Union

import asyncpg
from asyncpg import Connection

from settings import settings


class ExecutorService:

    async def execute_query(self, query: str, table: str) -> Union[List[Dict], Dict]:
        conn_string = f"{settings.db_driver}://{settings.db_user}:{settings.db_password}@" \
                      f"{settings.db_host}:{settings.db_port}/{table}"
        conn = await asyncpg.connect(conn_string)
        if 'select' in query.lower():
            data = await self._process_select(conn, query)
            # 'SELECT * FROM dv_raw.case_hub LIMIT 5;'
        elif 'delete' in query.lower() or 'update' in query.lower():
            async with conn.transaction():
                await conn.execute(query)
                data = {"result": "success"}
        return data

    async def _process_select(self, conn: Connection, query: str) -> List[Dict]:
        rows = await conn.fetch(query)
        return [dict(row) for row in rows]
