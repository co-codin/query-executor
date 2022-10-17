from typing import Dict, List, Union

import asyncpg
from asyncpg import Connection


class ExecutorService:

    async def execute_query(self, query: str) -> Union[List[Dict], Dict]:
        conn = await asyncpg.connect("postgresql://i10:BqY2EiQtaw9qeY8jPdiVCV2BS@10.16.22.24:5432/dwh_i10_dev")
        if 'select' in query.lower():
            data = await self.process_select(conn, query)
            # 'SELECT * FROM dv_raw.case_hub LIMIT 5;'
        elif 'delete' in query.lower() or 'update' in query.lower():
            async with conn.transaction():
                await conn.execute(query)
                data = {"result": "success"}
        return data

    async def process_select(self, conn: Connection, query: str) -> List[Dict]:
        rows = await conn.fetch(query)
        return [dict(row) for row in rows]
