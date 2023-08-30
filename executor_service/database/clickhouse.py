import logging
import pandas as pd
import numpy as np

from typing import Iterable

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from executor_service.settings import settings

LOG = logging.getLogger(__name__)


class ClickhouseService:
    def __init__(self):
        self._conn_string = settings.clickhouse_connection_string
        self.client: Client | None = None

    def connect(self):
        self.client = get_client(dsn=settings.clickhouse_connection_string)
        return self

    def create_publish_table(self, publish_name: str, df: pd.DataFrame):
        self._ping()
        col_names_and_types = self._get_col_names_and_types_from_df(df)
        schema = ','.join(col_names_and_types)
        db = settings.clickhouse_connection_string.rsplit('/', maxsplit=1)[1]
        self.client.command(
            f"""
                CREATE OR REPLACE TABLE {{db:Identifier}}.{{table:Identifier}} (id UInt64, {schema})
                ENGINE MergeTree()
                ORDER BY id
            """,
            parameters={'db': db, 'table': publish_name}
        )

    def create_db(self):
        db_name = self._conn_string.rsplit('/', maxsplit=1)[1]
        self.client.command('CREATE DATABASE IF NOT EXISTS {{name}}', parameters={'name': db_name})

    def _get_col_names_and_types_from_df(self, df: pd.DataFrame) -> Iterable[str]:
        df_json = df.head(2).to_json(orient="records")
        res = self.client.query(
            'DESC format(JSONEachRow, {df_json:String})',
            parameters={'df_json': df_json}
        )
        col_names_and_types = (
            f"{res_tuple[0]} {res_tuple[1]}"
            for res_tuple in reversed(res.result_rows)
        )
        return col_names_and_types

    def insert_dataframe(self, table: str, df: pd.DataFrame):
        self._ping()
        df['id'] = np.arange(1, df.shape[0] + 1)
        db = settings.clickhouse_connection_string.rsplit('/', maxsplit=1)[1]
        self.client.insert_df(f'{db}.{table}', df)

    def exist(self, publish_name) -> bool:
        self._ping()
        db = settings.clickhouse_connection_string.rsplit('/', maxsplit=1)[1]
        res = self.client.query(
            "EXISTS TABLE {db:Identifier}.{table:Identifier}",
            parameters={'db': db, 'table': publish_name}
        )
        return res.result_rows[0][0]

    def _ping(self):
        try:
            self.client.query('SELECT 1')
        except ClickHouseError:
            self.connect()


clickhouse_client = ClickhouseService().connect()
