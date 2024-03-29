from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from alembic.config import Config
from alembic import command

from clickhouse_connect import get_client
from executor_service.settings import settings

engine = create_engine(settings.db_connection_string_results)


if not database_exists(engine.url):
    print("DB does not exist, creating...")
    create_database(engine.url)
else:
    print("DB already exists, skipping")

engine = create_engine(settings.db_migration_connection_string)

if not database_exists(engine.url):
    print("DB does not exist, creating...")
    create_database(engine.url)
else:
    print("DB already exists, skipping")

alembic_cfg = Config("alembic.ini")
command.upgrade(alembic_cfg, "head")


conn_string, db_name = settings.clickhouse_connection_string.rsplit('/', maxsplit=1)
print(f'Creating clickhouse db: {db_name}')

clickhouse_client = get_client(dsn=conn_string)
clickhouse_client.command('CREATE DATABASE IF NOT EXISTS {name:Identifier}', parameters={'name': db_name})
