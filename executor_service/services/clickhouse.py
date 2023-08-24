import logging
import json
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
        self._conn_string = settings.db_sources_clickhouse
        self.client: Client | None = None

    def connect(self):
        self.client = get_client(dsn=settings.db_sources_clickhouse)
        return self

    def create_publish_table(self, publish_name: str, df: pd.DataFrame):
        self._ping()
        col_names_and_types = self._get_col_names_and_types_from_df(df)
        schema = ','.join(col_names_and_types)
        db = settings.db_sources_clickhouse.rsplit('/', maxsplit=1)[1]
        self.client.command(
            f"""
                CREATE OR REPLACE TABLE {db}.{publish_name} (id UInt64, {schema})
                ENGINE MergeTree()
                ORDER BY id
            """
        )

    def _get_col_names_and_types_from_df(self, df: pd.DataFrame) -> Iterable[str]:
        df_json = df.head(2).to_json(orient="records")
        res = self.client.query(
            f"""
                DESC format(JSONEachRow, {json.dumps(df_json)})
            """
        )
        col_names_and_types = (
            f"{res_tuple[0]} {res_tuple[1]}"
            for res_tuple in reversed(res.result_rows)
        )
        return col_names_and_types

    def insert_dataframe(self, table: str, df: pd.DataFrame):
        self._ping()
        df['id'] = np.arange(1, df.shape[0] + 1)
        db = settings.db_sources_clickhouse.rsplit('/', maxsplit=1)[1]
        self.client.insert_df(f'{db}.{table}', df)

    def exist(self, publish_name) -> bool:
        self._ping()
        res = self.client.query("EXISTS TABLE {table:Identifier}", parameters={'table': f'{publish_name}'})
        return res.result_rows[0][0]

    def _ping(self):
        try:
            self.client.query('SELECT 1')
        except ClickHouseError:
            self.connect()
