import asyncio
import os
import csv
import json
import logging
import secrets
import string
import psycopg

from datetime import datetime
from itertools import zip_longest
from tempfile import TemporaryDirectory
from typing import Iterable, Any

from psycopg import sql as sql_builder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from fastapi import HTTPException, status

from executor_service.settings import settings

from executor_service._msgpack_io import msgpack_reader
from executor_service.database import AsyncSession
from executor_service.errors import QueryNotFoundError, QueryNotRunning
from executor_service.mq import create_channel
from executor_service.models.queries import QueryExecution, QueryStatus, QueryDestinationStatus
from executor_service.database import db_session
from executor_service.services.query_runner import QueryRunnerFactory
from executor_service.services.crypto import decrypt


LOG = logging.getLogger(__name__)


ORDER_KEY = '__dwh_seq__'
CREDENTIALS_SYMBOLS = string.ascii_letters + string.digits + '+-=/,.'


def _generate_random_string(strength: int):
    return ''.join(secrets.choice(CREDENTIALS_SYMBOLS) for _ in range(strength))


async def get_query_result(db_table: str, limit: int, offset: int) -> list[dict]:
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


async def delete_query_execs(db_tables: list[str]) -> None:
    if not db_tables:
        return 
    tables = sql_builder.SQL(', ').join(map(sql_builder.Identifier, db_tables))
    drop_tables_sql = sql_builder.SQL('DROP TABLE IF EXISTS {}').format(tables)

    async with await psycopg.AsyncConnection.connect(settings.db_connection_string_results) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(drop_tables_sql)


async def _get_query_by_id(session: AsyncSession, query_id: int) -> QueryExecution:
    query = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.id == query_id))
    query = query.scalars().first()
    if not query:
        raise QueryNotFoundError(query_id=query_id)
    return query


async def _get_query_by_guid(query_guid: str, session: AsyncSession) -> QueryExecution:
    query = await session.execute(
        select(QueryExecution)
        .options(selectinload(QueryExecution.results))
        .where(QueryExecution.guid == query_guid))
    query = query.scalars().first()
    if not query:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return query


async def terminate_query(query_guid: str):
    async with db_session() as session:
        query = await _get_query_by_guid(query_guid, session)
        if query.status != QueryStatus.RUNNING.value:
            raise QueryNotRunning(query_id=query.id)

        query_runner = QueryRunnerFactory.build(
            query_id=query.id,
            conn_string=decrypt(settings.encryption_key, query.db)
        )

        # в том случае, если процесс выполнения запроса работает,
        # его отмена из другого выдаст ошибку в первом
        # это будет работать в случае запуска сервиса в нескольких процессах или на нескольких нодах
        # также существует риск рейс кондишена когда первый процесс уже упал,
        # а второй еще не установил статус в Cancelled,
        # в таком случае первый процесс совершенно законно поставит статус ошибки
        # чтобы этого избежать нужно сделать быстрый row-lock на отмену pg бакенда и установки статуса

        await session.refresh(query, with_for_update=True)
        await query_runner.cancel(query_guid=query.guid)
        query.status = QueryStatus.CANCELLED.value
        await session.commit()
        return query


async def execute_query(query_id: int, conn_string: str):
    """
    Выполняет запрос и загружает его результаты
    :param query_id: Идентификатор запроса
    :param conn_string: Строка подключения к базе данных
    """
    try:
        await _execute_query(query_id, conn_string)
    except Exception as e:
        LOG.exception(f'Failed to execute run {query_id}: {repr(e)}')


async def _execute_query(query_id: int, conn_string: str):
    async with db_session() as session:
        query = await _get_query_by_id(session, query_id)
        query.status = QueryStatus.RUNNING.value
        await session.commit()

        with TemporaryDirectory() as temp_dir:
            data_path = os.path.join(temp_dir, f'{query.guid}.bin')

            try:
                await _execute_sql_to_file(query, conn_string, write_to=data_path)
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
                    LOG.info(f'Run {query.id} upload to {dest.dest_type} started')
                    path, creds = await load(query, data_path)
                    LOG.info(f'Run {query.id} upload to {dest.dest_type} finished')
                except Exception:
                    LOG.exception(f'Failed to upload result of query {query.guid} into {dest.dest_type}')
                    dest.status = QueryDestinationStatus.ERROR.value
                    dest.error_description = f'Failed to upload into {dest.dest_type}'
                    query.status = QueryStatus.ERROR.value
                    query.error_description = f'Results failed to upload into {dest.dest_type}'
                    await session.commit()
                    await send_notification(query)
                    return

                dest.path = path
                dest.status = QueryDestinationStatus.UPLOADED.value
                dest.finished_at = datetime.utcnow()
                dest.access_creds = json.dumps(creds)
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
                'run_id': query.id,
                'status': query.status,
                'error_description': query.error_description
            })
        )


async def _load_into_table(query, write_from):
    with msgpack_reader(write_from) as reader:
        names = reader.readrow()
        types = reader.readrow()

        if ORDER_KEY in names:
            raise Exception('Failed to insert order key')

        table_name = f'results_{int(query.id)}'
        # для пагинации результатов нужен order, добавляем дополнительное поле
        ddl = [f'CREATE TABLE IF NOT EXISTS {table_name} ({ORDER_KEY} BIGSERIAL PRIMARY KEY,']
        fields = []
        for name, type_ in zip_longest(names, types):
            fields.append(f'"{name}" {type_} NULL')
        ddl.append(','.join(fields))
        ddl.append(')')

        ddl = ''.join(ddl)
        LOG.info(f'DDL for query {query.id}: {ddl}', extra={'query': query.guid})

        async with await psycopg.AsyncConnection.connect(settings.db_connection_string_results,
                                                         cursor_factory=psycopg.AsyncClientCursor) as con:
            async with con.cursor() as cursor:
                await cursor.execute(ddl)
                user_name = f'sdwh_run_{query.id}'
                user_pass = _generate_random_string(8)
                await cursor.execute(f"CREATE USER {user_name} WITH password %s", [user_pass])
                await cursor.execute(f"GRANT SELECT ON {table_name} TO {user_name}")

                for batch_records in to_batches(100, reader):
                    await _insert_many(cursor, table_name, names, batch_records)

    return table_name, {
        'user': user_name,
        'pass': user_pass
    }


async def _insert_many(cursor, table_name, field_names, records):
    values = []
    values_template = ','.join(['%s'] * len(field_names))

    for r in records:
        row = [None if col == 'None' else col for col in r]
        value = cursor.mogrify(f'({values_template})', row)
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
}


async def _execute_sql_to_file(query, conn_string: str, write_to):
    query_runner = QueryRunnerFactory.build(query_id=query.id, conn_string=conn_string)

    LOG.info(f'Run {query.id} using db {conn_string}')
    await query_runner.execute_to_file(query.query, write_to)


def to_batches(size: int, iterable: Iterable) -> Iterable[list[Any]]:
    batch_records = []
    for record in iterable:
        batch_records.append(record)
        if len(batch_records) >= size:
            yield batch_records
            batch_records.clear()

    if batch_records:
        yield batch_records
