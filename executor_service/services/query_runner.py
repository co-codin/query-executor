import logging
import psycopg
import asyncio
import clickhouse_connect

from abc import ABC, abstractmethod
from typing import List, AsyncIterable
from clickhouse_connect.driver.exceptions import DatabaseError

from executor_service._msgpack_io import msgpack_writer
from executor_service.errors import QueryNotRunning
from executor_service.settings import settings

LOG = logging.getLogger(__name__)


class QueryRunner(ABC):
    @abstractmethod
    def __init__(self, query_id: int, conn_string: str):
        self._query_id = query_id
        self._conn_string = conn_string

    @property
    def db_app_name(self):
        return f'sdwh_{self._query_id}'

    @abstractmethod
    async def execute_to_file(self, query: str, write_to: str):
        ...

    @abstractmethod
    async def cancel(self, query_guid: str):
        ...

    @staticmethod
    async def _save_to_dir(write_to: str, col_names: List, col_types: List, rows: AsyncIterable):
        with msgpack_writer(write_to) as writer:
            # first two rows name and types
            writer.writerow(col_names)
            writer.writerow(col_types)

            async for record in rows:
                writer.writerow(record)


class PostgresRunner(QueryRunner):
    def __init__(self, query_id: int, conn_string: str):
        super().__init__(query_id, conn_string)

    async def execute_to_file(self, query: str, write_to: str):
        async with await psycopg.AsyncConnection.connect(f'{self._conn_string}?application_name={self.db_app_name}') as conn:
            async with conn.cursor(f'server_cursor_{self._query_id}') as cursor:
                await cursor.execute(query)
                col_names = [c.name for c in cursor.description]
                col_types = [c._type_display() for c in cursor.description]
                await self._save_to_dir(write_to, col_names=col_names, col_types=col_types, rows=cursor)

    async def cancel(self, query_guid: str):
        async with await psycopg.AsyncConnection.connect(self._conn_string) as conn:
            async with conn.cursor() as cursor:
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


class ClickHouseRunner(QueryRunner):
    def __init__(self, query_id: int, conn_string: str):
        super().__init__(query_id, conn_string)

    @staticmethod
    async def _row_gen(rows):
        for row in rows:
            yield row

    async def execute_to_file(self, query: str, write_to: str):
        query_result = await asyncio.to_thread(self._execute_to_file, query)
        col_names = query_result.column_names
        col_types = [col_type.base_type for col_type in query_result.column_types]
        rows = self._row_gen(rows=query_result.result_rows)
        await self._save_to_dir(write_to, col_names=col_names, col_types=col_types, rows=rows)

    def _execute_to_file(self, query: str):
        client = clickhouse_connect.get_client(dsn=self._conn_string)
        try:
            result = client.query(
                query,
                settings={'replace_running_query': 1, 'query_id': self.db_app_name}
            )
        except DatabaseError as db_err:
            if "Code: 394" in str(db_err):  # if query was canceled
                raise psycopg.errors.QueryCanceled()
            raise db_err
        finally:
            client.close()
        return result

    async def cancel(self, query_guid: str):
        await asyncio.to_thread(self._cancel, query_guid)

    def _cancel(self, query_guid: str):
        client = clickhouse_connect.get_client(dsn=self._conn_string)
        try:
            res = client.query(
                "select query_id, query "
                "from system.processes "
                "where query_id=%s",
                (self.db_app_name,)
            )
            try:
                row = res.first_row
            except IndexError:
                raise QueryNotRunning(query_id=self._query_id)

            pid, sql = row
            LOG.info(f'Cancelling query {self._query_id} {query_guid} having db pid {pid} running query {sql}')
            client.query(
                "kill query "
                "where query_id=%s",
                (pid,)
            )
        finally:
            client.close()


class QueryRunnerFactory:
    _SOURCES_TO_QUERY_RUNNER_TYPE = {
        'postgresql': PostgresRunner,
        'clickhouse': ClickHouseRunner
    }

    @classmethod
    def build(cls, query_id: int, conn_string: str) -> QueryRunner:
        driver = conn_string.split('://', maxsplit=1)[0]
        query_runner_class = cls._SOURCES_TO_QUERY_RUNNER_TYPE[driver]
        return query_runner_class(query_id=query_id, conn_string=conn_string)
