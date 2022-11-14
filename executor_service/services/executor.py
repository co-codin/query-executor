import os
import csv
import json
import logging
from itertools import zip_longest
from tempfile import TemporaryDirectory
from typing import Dict, List, Tuple

import psycopg

from settings import settings

from executor_service.errors import QueryNotFoundError
from executor_service.mq import create_channel
from executor_service.models.queries import Query
from executor_service.fs import fs_client


LOG = logging.getLogger(__name__)

QUERIES = {}


class NoResultsError(Exception):
    pass


class ExecutorService:

    async def execute_query(self, query: Query) -> Tuple[str, List[Dict]]:
        """
        Выполняет запрос в отдельной таске, возвращая управление не дожидаясь
        :param query: SQL запрос
        :param db: Кодовое име базы данных, в которой запрос должен быть выполнен
        :return: Возвращает PID запроса, для дальнейшего трекинга
        """
        conn_string = settings.db_sources[query.db]
        conn = await asyncpg.connect(conn_string, server_settings={
            'application_name': f'sdwh_{query.id}'
        })
        query_pid_result = await conn.execute("SELECT pg_backend_pid();")
        query_pid = query_pid_result.split()[-1]
        data = await self._process_select(conn, query)  # 'SELECT * FROM dv_raw.case_hub LIMIT 5;'
        global QUERIES
        QUERIES[f"{db}_{query_pid}"] = data
        pid_info = f"Your query pid is {query_pid}"
        return pid_info, data

    async def get_query_result(self, query_pid: int, table: str) -> List[Dict]:
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

    async def _process_select(self, conn, query: str) -> List[Dict]:
        rows = await conn.fetch(query)
        return [dict(row) for row in rows]


async def execute_query(query: Query):
    """
    Выполняет запрос в отдельной таске, возвращая управление не дожидаясь
    :param query: SQL запрос
    :param db: Кодовое име базы данных, в которой запрос должен быть выполнен
    :return: Возвращает PID запроса, для дальнейшего трекинга
    """
    await execute_query_into(query, destinations=('table', 'file'))
    await send_notification(query)


async def send_notification(query: Query):
    async with create_channel() as channel:
        await channel.basic_publish(
            exchange=settings.exchange_execute,
            routing_key='result',
            body=json.dumps({
                'guid': query.guid,
                'status': query.status,
            })
        )


async def execute_query_into(query, destinations=()):
    with TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, f'{query.guid}.csv')
        await _execute_sql_to_file(query, write_to=csv_path)

        for dest in destinations:
            load = RESULTS_DEST_MAPPING.get(dest)
            if load is None:
                LOG.error(f'Unknown destination type: {dest}')
                continue
            await load(query, csv_path)


async def _load_into_file(query, write_from):
    async with fs_client() as fs:
        await fs.ensure_bucket(settings.minio_bucket_name)
        await fs.upload_file(settings.minio_bucket_name, f'{query.guid}.csv', write_from)


async def _load_into_table(query, write_from):
    with open(write_from) as f:
        reader = csv.reader(f)
        rows = iter(reader)

        names = next(rows)
        types = next(rows)

        table_name = f'results_{int(query.id)}'
        ddl = [f'CREATE TABLE IF NOT EXISTS {table_name} (']
        fields = []
        for name, type_ in zip_longest(names, types):
            fields.append(f'"{name}" {type_}')
        ddl.append(','.join(fields))
        ddl.append(')')

        ddl = ''.join(ddl)
        LOG.info(f'DDL for query {query.id}: {ddl}', extra={'query': query.guid})

        async with await psycopg.AsyncConnection.connect(settings.db_connection_string_results,
                                                         cursor_factory=psycopg.AsyncClientCursor) as con:
            async with con.cursor() as cursor:
                await cursor.execute(ddl)

                batch_size = 100
                batch_records = []

                while True:
                    try:
                        record = next(rows)
                        batch_records.append(record)

                        if len(batch_records) > batch_size:
                            await _insert_many(cursor, table_name, names, batch_records)
                    except StopIteration:
                        break

                if batch_records:
                    await _insert_many(cursor, table_name, names, batch_records)


async def _insert_many(cursor, table_name, field_names, records):
    values = []
    values_template = ','.join(['%s'] * len(field_names))

    for r in records:
        value = cursor.mogrify(f'({values_template})', r)
        values.append(value)

    sql = ''.join([
        f'INSERT INTO {table_name} (',
        ','.join([f'"{name}"' for name in field_names]),
        ') VALUES ',
        ','.join(values)
    ])
    await cursor.execute(sql)


RESULTS_DEST_MAPPING = {
    'table': _load_into_table,
    'file': _load_into_file,
}


async def _execute_sql_to_file(query, write_to):
    conn_string = settings.db_sources[query.db]
    async with await psycopg.AsyncConnection.connect(f'{conn_string}?application_name=sdwh_{query.id}') as con:
        async with con.cursor(f'server_cursor_{query.id}') as cursor:
            await cursor.execute(query.query)

            with open(write_to, 'w') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

                # first two rows name and types
                writer.writerow([c.name for c in cursor.description])
                writer.writerow([c._type_display() for c in cursor.description])

                batch_size = 100
                batch_records = []
                async for record in cursor:
                    batch_records.append(record)
                    if len(batch_records) > batch_size:
                        writer.writerows(batch_records)
                        batch_records.clear()

                if batch_records:
                    writer.writerows(batch_records)
