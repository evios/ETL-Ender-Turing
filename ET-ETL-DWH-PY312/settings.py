from pathlib import Path
from urllib.parse import quote

from pydantic import field_validator
from pydantic_settings import BaseSettings
import logging
from typing import Optional



class Settings(BaseSettings):
    # Logging
    log_fpath: Path = Path("./logs.log")
    log_level: str = "INFO"
    log_format: str = '%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'

    log_every: int = 250  # for detailed requests we log some progress, but only on each N fetched
    test_mode: bool = False
    test_mode_limit_sessions: int = 200

    # ETL settings
    incremental_sync_n_days: int = 30
    # File path to store the datetime
    last_synced_fpath: Path = Path('./last_synced.json')

    # Extract settings - Ender Turing params
    et_domain: str = None
    et_user: str = None
    et_password: Optional[str] = None
    et_token: str | None = None
    et_auth_by_token: bool = True

    @field_validator("et_password")
    def encode_password(cls, v) -> str:
        if v:
            return quote(v)

    # Load settings
    init_db_tables: bool = True
    # DEV DB example - local sqlite, will be overwritten by ENV variable in PROD run
    DATABASE_URL: str = "sqlite:///dev/dev-db.sqlite"
    # DATABASE_URL: str = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"

    class Config:
        case_sensitive = False
        env_file = ".env"


settings = Settings()

# Setup Logging to Console + File
logger = logging.getLogger(__name__)
logger.setLevel(settings.log_level)
# Create console and file handlers
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler(settings.log_fpath)
# Create a logging format
formatter = logging.Formatter(settings.log_format)
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
# Add handlers to the logger
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
