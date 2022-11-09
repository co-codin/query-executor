from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Logging constants"""
    log_dir: str = "logs"
    time_period_unit: str = "D"
    backup_count: int = 5
    date_time_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = "utf-8"

    db_sources: dict = {
        'raw': 'postgresql://postgres:dwh@db:5432/postgres'
    }

    mq_connection_string: str = 'amqp://dwh:dwh@rabbit:5672'
    exchange_execute = 'query_execute'

    class Config:
        env_prefix = ""
        case_sensitive = False
        env_file = "../.env"
        env_file_encoding = "utf-8"


settings = Settings()
