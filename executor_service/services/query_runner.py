import logging
import psycopg

from abc import ABC, abstractmethod

from executor_service._msgpack_io import msgpack_writer
from executor_service.errors import QueryNotRunning
from executor_service.settings import settings

LOG = logging.getLogger(__name__)


class QueryRunner(ABC):
    @abstractmethod
    def __init__(self, query_id: int):
        self._query_id = query_id

    @abstractmethod
    async def execute_to_file(self, query: str, write_to: str):
        ...

    @abstractmethod
    async def cancel(self, query_guid: str):
        ...

    @staticmethod
    async def _save_to_dir(write_to: str, cursor):
        with msgpack_writer(write_to) as writer:
            # first two rows name and types
            writer.writerow([c.name for c in cursor.description])
            writer.writerow([c._type_display() for c in cursor.description])

            async for record in cursor:
                writer.writerow(record)

    @property
    def db_app_name(self):
        return f'sdwh_{self._query_id}'


class PostgresRunner(QueryRunner):
    def __init__(self, query_id: int):
        super().__init__(query_id)
        self._conn_string = settings.db_sources['raw']

    async def execute_to_file(self, query: str, write_to: str):
        async with await psycopg.AsyncConnection.connect(f'{self._conn_string}?application_name={self.db_app_name}') as conn:
            async with conn.cursor(f'server_cursor_{self._query_id}') as cursor:
                await cursor.execute(query)
                await self._save_to_dir(write_to, cursor)

    async def cancel(self, query_guid: str):
        async with await psycopg.AsyncConnection.connect(self._conn_string) as con:
            async with con.cursor() as cursor:
                await cursor.execute(
                    """
                    select pid, backend_start, query from pg_stat_activity
                    where state='active' and application_name=%s
                    """, [self.db_app_name]
                )
                row = await cursor.fetchone()
                if not row:
                    raise QueryNotRunning(query_id=self._query_id)

                pid, started_at, sql = row

                LOG.info(f'Cancelling query {self._query_id} {query_guid} having db pid {pid} running query {sql}')
                await cursor.execute("SELECT pg_cancel_backend(%s)", [pid])


class ClickhouseRunner(QueryRunner):
    def __init__(self, id_, query):
        super().__init__(id_, query)

    async def execute_to_file(self, write_to):
        ...

    async def cancel(self):
        ...
