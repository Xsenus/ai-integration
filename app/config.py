# app/config.py
from __future__ import annotations
from typing import Final, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Общие настройки приложения и подключений к БД."""
    # === DB DSNs (SQLAlchemy async: postgresql+asyncpg://user:pass@host:port/db) ===
    BITRIX_DATABASE_URL: Optional[str] = None
    PARSING_DATABASE_URL: Optional[str] = None
    PP719_DATABASE_URL: Optional[str] = None
    POSTGRES_DATABASE_URL: Optional[str] = None  # основная 'postgres'

    # === Logging / SQL ===
    LOG_LEVEL: str = "INFO"
    ECHO_SQL: bool = False

    # === External services ===
    DADATA_API_KEY: Optional[str] = None
    DADATA_SECRET_KEY: Optional[str] = None
    SCRAPERAPI_KEY: Optional[str] = None

    # === External analyze service (для POST /v1/lookup/ai-analyzer) ===
    # Можно указать любой из этих ключей в .env; приоритет у AI_ANALYZE_BASE/AI_ANALYZE_TIMEOUT
    AI_ANALYZE_BASE: Optional[str] = None     # напр.: http://37.221.125.221:8123
    ANALYZE_BASE: Optional[str] = None        # альтернативное имя переменной окружения
    AI_ANALYZE_TIMEOUT: int = 30              # таймаут по умолчанию, сек
    ANALYZE_TIMEOUT: Optional[int] = None     # альтернативное имя переменной окружения

    # === Scrape params (используются в services/scrape.py) ===
    PARSE_MAX_CHUNK_SIZE: int = 100_000
    PARSE_MIN_HTML_LEN: int = 100

    # Можно хранить CORS-настройки в .env (используются через getattr в app/main.py)
    CORS_ALLOW_ORIGINS: Optional[str] = None
    CORS_ALLOW_METHODS: Optional[str] = None
    CORS_ALLOW_HEADERS: Optional[str] = None
    CORS_ALLOW_CREDENTIALS: Optional[bool] = None

    B24_BASE_URL: str | None = None
    B24_PAGE_SIZE: int = 200
    B24_BATCH_ENABLED: bool = True
    B24_BATCH_SIZE: int = 25
    B24_SYNC_ENABLED: bool = True
    B24_SYNC_INTERVAL: int = 600
    B24_SYNC_COMMIT_BATCH: int = 200

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- Convenience props ----
    @property
    def bitrix_url(self) -> Optional[str]:
        return self.BITRIX_DATABASE_URL

    @property
    def parsing_url(self) -> Optional[str]:
        return self.PARSING_DATABASE_URL

    @property
    def pp719_url(self) -> Optional[str]:
        return self.PP719_DATABASE_URL

    @property
    def postgres_url(self) -> Optional[str]:
        return self.POSTGRES_DATABASE_URL

    @property
    def analyze_base(self) -> Optional[str]:
        """Базовый URL внешнего сервиса анализа (берёт AI_ANALYZE_BASE или ANALYZE_BASE)."""
        return self.AI_ANALYZE_BASE or self.ANALYZE_BASE

    @property
    def analyze_timeout(self) -> int:
        """Таймаут ожидания ответа внешнего сервиса, сек."""
        return int(self.AI_ANALYZE_TIMEOUT or self.ANALYZE_TIMEOUT or 30)


settings: Final[Settings] = Settings()
