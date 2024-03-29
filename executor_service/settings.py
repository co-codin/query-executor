from pydantic import BaseSettings


class Settings(BaseSettings):
    # Uvicorn constants
    port: int = 8000

    # Logging constants
    debug: bool = False

    log_dir: str = "/var/log/n3dwh/"
    log_name: str = "query_executor.log"
    time_period_unit: str = "D"
    backup_count: int = 5
    date_time_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = "utf-8"

    db_connection_string: str = 'postgresql+asyncpg://postgres:dwh@db.lan:5432/executor'
    db_migration_connection_string: str = 'postgresql+psycopg2://postgres:dwh@db.lan:5432/executor'
    db_connection_string_results: str = 'postgresql://postgres:dwh@db.lan:5432/results'
    api_iam = 'http://iam.lan:8000'

    mq_connection_string: str = 'amqp://dwh:dwh@rabbit.lan:5672'
    exchange_execute = 'query_execute'

    thread_pool_size = 100
    encryption_key: str = '154de72125d4c917bd0764f09bc6af6265b28cd11da2efb659151ac02c7ca0d3'

    clickhouse_connection_string: str = 'clickhouse://clickhouse:dwh@clickhouse.lan:8123/dwh'
    origins: list[str] = [
        '*'
    ]

    publish_exchange: str = 'publish_exchange'
    publish_request_queue: str = 'publish_requests'
    publish_result_queue: str = 'publish_results'
    
    class Config:
        env_prefix = "dwh_query_executor_"
        case_sensitive = False
        env_file = "../.env"
        env_file_encoding = "utf-8"


settings = Settings()
