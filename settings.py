from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Logging constants"""
    debug = True

    log_dir: str = "/var/log/n3dwh/"
    log_name: str = "query_executor.json"
    time_period_unit: str = "D"
    backup_count: int = 5
    date_time_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = "utf-8"

    db_connection_string: str = 'postgresql+asyncpg://postgres:dwh@db:5432/executor'
    db_sources: dict = {
        'raw': 'postgresql://postgres:dwh@db:5432/postgres'
    }
    db_connection_string_results: str = 'postgresql://postgres:dwh@db:5432/results'

    minio_host: str = 'minio:9000'
    minio_access_key: str = 'dwh_access_key'
    minio_secret_key: str = 'dwh_secret_key'
    minio_bucket_name: str = 'results'

    mq_connection_string: str = 'amqp://dwh:dwh@rabbit:5672'
    exchange_execute = 'query_execute'

    thread_pool_size = 100

    class Config:
        env_prefix = ""
        case_sensitive = False
        env_file = "../.env"
        env_file_encoding = "utf-8"


settings = Settings()
