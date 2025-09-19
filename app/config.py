from __future__ import annotations
from typing import Final
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # DSN
    BITRIX_DATABASE_URL: str | None = None
    PARSING_DATABASE_URL: str | None = None
    PP719_DATABASE_URL: str | None = None

    # Logs
    LOG_LEVEL: str = "INFO"

    # SQLAlchemy / DB
    ECHO_SQL: bool = False

    # DaData
    DADATA_API_KEY: str | None = None
    DADATA_SECRET_KEY: str | None = None

    # Scraper
    SCRAPERAPI_KEY: str | None = None
    PARSE_MAX_CHUNK_SIZE: int = 100_000
    PARSE_MIN_HTML_LEN: int = 100

    # CORS
    CORS_ALLOW_ORIGINS: str | None = None
    CORS_ALLOW_METHODS: str | None = "GET,POST,OPTIONS"
    CORS_ALLOW_HEADERS: str | None = "Authorization,Content-Type"
    CORS_ALLOW_CREDENTIALS: bool = False

    # Observability
    METRICS_ENABLED: bool = False
    SENTRY_DSN: str | None = None
    REQUEST_LOG_SAMPLE_RATE: float = 1.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def bitrix_url(self) -> str | None: return self.BITRIX_DATABASE_URL
    @property
    def parsing_url(self) -> str | None: return self.PARSING_DATABASE_URL
    @property
    def pp719_url(self) -> str | None:   return self.PP719_DATABASE_URL
    
    @property
    def bootstrap_dsn(self) -> str | None:
        """
        Нужен только для asyncpg.connect() в wait_for_postgres.
        Строим из BITRIX_DATABASE_URL, просто убирая '+asyncpg'.
        Пример: postgresql+asyncpg://... -> postgresql://...
        """
        if not self.BITRIX_DATABASE_URL:
            return None
        return self.BITRIX_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)

settings: Final[Settings] = Settings()