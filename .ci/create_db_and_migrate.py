from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from alembic.config import Config
from alembic import command
from executor_service.database.clickhouse import clickhouse_client
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

print('Creating clickhouse db')
clickhouse_client.create_db()
