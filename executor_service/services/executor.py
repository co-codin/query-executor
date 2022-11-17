import os
import csv
import json
import logging
from itertools import zip_longest
from tempfile import TemporaryDirectory
from typing import Dict, List

import psycopg
from psycopg import sql as sql_builder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from settings import settings

from executor_service.database import AsyncSession
from executor_service.errors import QueryNotFoundError, QueryNotRunning
from executor_service.mq import create_channel
from executor_service.models.queries import QueryExecution, QueryStatus, QueryDestinationStatus
from executor_service.fs import fs_client
from executor_service.database import db_session


LOG = logging.getLogger(__name__)


ORDER_KEY = '__dwh_seq__'


async def get_query_result(db_table: str, limit: int, offset: int) -> List[Dict]:
    sql = sql_builder.SQL('SELECT * FROM {} ORDER BY {} LIMIT %s OFFSET %s').format(
        sql_builder.Identifier(db_table),
        sql_builder.Identifier(ORDER_KEY)
    )
    async with await psycopg.AsyncConnection.connect(settings.db_connection_string_results) as con:
        async with con.cursor() as cursor:
            await cursor.execute(sql, (limit, offset))
            fields = [c.name for c in cursor.description]

            result = []
            async for row in cursor:
                item = dict(zip_longest(fields, row))
                del item[ORDER_KEY]
                result.append(item)

    return result


async def _get_query(session: AsyncSession, query_id: int) -> QueryExecution:
    query = await session.execute(
        select(QueryExecution)
            .options(selectinload(QueryExecution.results))
            .where(QueryExecution.id == query_id))
    query = query.scalars().first()
    if not query:
        raise QueryNotFoundError(query_id=query_id)
    return query


async def terminate_query(query_id: int):
    async with db_session() as session:
        query = await _get_query(session, query_id)
        if query.status != QueryStatus.RUNNING.value:
            raise QueryNotRunning(query_id=query_id)

        conn_string = settings.db_sources[query.db]
        async with await psycopg.AsyncConnection.connect(conn_string) as con:
            async with con.cursor() as cursor:
                await cursor.execute(
                    """
                    select pid, backend_start, query from pg_stat_activity
                    where state='active' and application_name=%s
                    """, [db_app_name(query)]
                )
                row = await cursor.fetchone()
                if not row:
                    raise QueryNotRunning(query_id=query_id)

                pid, started_at, sql = row

                LOG.info(f'Cancelling query {query.id} {query.guid} having db pid {pid} running query {sql}')

                # в том случае, если процесс выполнения запроса работает,
                # его отмена из другого выдаст ошибку в первом
                # это будет работать в случае запуска сервиса в нескольких процессах или на нескольких нодах
                # также существует риск рейс кондишена когда первый процесс уже упал,
                # а второй еще не установил статус в Cancelled,
                # в таком случае первый процесс совершенно законно поставит статус ошибки
                # чтобы этого избежать нужно сделать быстрый row-lock на отмену pg бакенда и установки статуса
                await session.refresh(query, with_for_update=True)
                await cursor.execute("SELECT pg_cancel_backend(%s)", [pid])
                query.status = QueryStatus.CANCELLED.value
                await session.commit()


async def execute_query(query_id: int):
    """
    Выполняет запрос и загружает его результаты
    :param query_id: Идентификатор запроса
    """
    async with db_session() as session:
        query = await _get_query(session, query_id)
        query.status = QueryStatus.RUNNING.value
        await session.commit()

        with TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, f'{query.guid}.csv')

            try:
                await _execute_sql_to_file(query, write_to=csv_path)
            except psycopg.errors.QueryCanceled:
                await session.refresh(query, with_for_update=True)
                if query.status == QueryStatus.CANCELLED.value:
                    # нормальное поведение при отмене выполнения
                    return
                await session.commit()
                LOG.error(f'Query {query.id} was cancelled')
                query.status = QueryStatus.ERROR.value
                query.error_description = f'Cancelled'
                await session.commit()
                await send_notification(query)
                return
            except Exception:
                LOG.exception(f'Failed to run query: {query.guid}')
                query.status = QueryStatus.ERROR.value
                query.error_description = f'SQL execution failed'
                await session.commit()
                await send_notification(query)
                return

            for dest in query.results:
                load = RESULTS_DEST_MAPPING.get(dest.dest_type)
                if load is None:
                    LOG.error(f'Unknown destination type: {dest.dest_type}')
                    continue
                try:
                    path = await load(query, csv_path)
                except Exception:
                    LOG.exception(f'Failed to upload result of query {query.guid} into {dest}')
                    dest.status = QueryDestinationStatus.ERROR.value
                    dest.error_description = f'Failed to upload into {dest.dest_type}'
                    query.status = QueryStatus.ERROR.value
                    query.error_description = f'Results failed to upload into {dest.dest_type}'
                    await session.commit()
                    await send_notification(query)
                    return

                dest.path = path
                dest.status = QueryDestinationStatus.UPLOADED.value
                await session.commit()

        query.status = QueryStatus.DONE.value
        await session.commit()

    await send_notification(query)


async def send_notification(query: QueryExecution):
    async with create_channel() as channel:
        await channel.basic_publish(
            exchange=settings.exchange_execute,
            routing_key='result',
            body=json.dumps({
                'guid': query.guid,
                'status': query.status,
                'error_description': query.error_description
            })
        )


async def _load_into_file(query, write_from):
    async with fs_client() as fs:
        await fs.ensure_bucket(settings.minio_bucket_name)
        await fs.upload_file(settings.minio_bucket_name, f'{query.guid}.csv', write_from)

    return f'{settings.minio_bucket_name}/{query.guid}.csv'


async def _load_into_table(query, write_from):
    with open(write_from) as f:
        reader = csv.reader(f)
        rows = iter(reader)

        names = next(rows)
        types = next(rows)

        if ORDER_KEY in names:
            raise Exception('Failed to insert order key')

        table_name = f'results_{int(query.id)}'
        # для пагинации результатов нужен order, добавляем дополнительное поле
        ddl = [f'CREATE TABLE IF NOT EXISTS {table_name} ({ORDER_KEY} BIGSERIAL PRIMARY KEY,']
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

    return table_name


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


def db_app_name(query: QueryExecution):
    return f'sdwh_{query.id}'


async def _execute_sql_to_file(query, write_to):
    conn_string = settings.db_sources[query.db]
    async with await psycopg.AsyncConnection.connect(f'{conn_string}?application_name={db_app_name(query)}') as con:
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
