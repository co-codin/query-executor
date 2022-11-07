from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Logging constants"""
    log_dir: str = "logs"
    time_period_unit: str = "D"
    backup_count: int = 5
    date_time_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = "utf-8"

    """DB constants"""
    db_driver: str = Field("postgres")
    db_host: str = Field("postgres")
    db_port: str = Field("5432")
    db_name: str = Field("postgres")
    db_user: str = Field("postgres")
    db_password: str = Field("postgres")

    class Config:
        env_prefix = ""
        case_sensitive = False
        env_file = "../.env"
        env_file_encoding = "utf-8"


settings = Settings()
