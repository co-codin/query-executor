import logging
import clickhouse_connect

from executor_service.settings import settings

LOG = logging.getLogger(__name__)


class ClickhouseService:
    def __init__(self):
        self._conn_string = settings.db_sources_clickhouse
        self.client = None

    def connect(self):
        self.client = clickhouse_connect.get_client(dsn=settings.db_sources_clickhouse)

    def createPublishTable(self, guid: str, schema: str):
        self.client.command(
            f'CREATE TABLE IF NOT EXISTS publish_{guid} ({schema}) ENGINE MergeTree'
        )

    async def dropPublishTable(self, guid: str):
        self.client.command(
            "DROP TABLE {}_{}".format("publish", guid)
        )

    def execute(self, command: str):
        self.client.command(command)

    def insert(self, guid, query_id, desk_type, path, published_at, status, finished_at, error_description, access_creds):
        data = [[query_id, desk_type, path, published_at, status, finished_at, error_description, access_creds]]
        self.client.insert('publish_'+guid, data, column_names=['query_id', 'desk_type', 'path', 'published_at', 'status','finished_at', 'error_description', 'access_creds'])

    def getByName(self, guid, publish_name):
        return self.client.query(
            "SELECT * FROM publish_{} WHERE publish_name = '{}'".format(guid, publish_name)
        )
