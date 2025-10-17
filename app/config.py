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

    # === Внешний AI-сервис (JSON-интеграция) ===
    CHAT_MODEL: str = "gpt-4o"
    OPENAI_EMBED_MODEL: str = "text-embedding-3-large"
    EMBED_MODEL: Optional[str] = None

    IB_GOODS_TYPES_TABLE: str = "ib_goods_types"
    IB_GOODS_TYPES_ID_COLUMN: str = "id"
    IB_GOODS_TYPES_NAME_COLUMN: str = "goods_type_name"
    IB_GOODS_TYPES_VECTOR_COLUMN: str = "goods_type_vector"

    IB_EQUIPMENT_TABLE: str = "ib_equipment"
    IB_EQUIPMENT_ID_COLUMN: str = "id"
    IB_EQUIPMENT_NAME_COLUMN: str = "equipment_name"
    IB_EQUIPMENT_VECTOR_COLUMN: str = "equipment_vector"

    # === External analyze service (используется в /v1/analyze-json) ===
    # Можно указать любой из этих ключей в .env; приоритет у AI_ANALYZE_BASE/AI_ANALYZE_TIMEOUT
    AI_ANALYZE_BASE: Optional[str] = None     # напр.: http://37.221.125.221:8123
    ANALYZE_BASE: Optional[str] = None        # альтернативное имя переменной окружения
    AI_ANALYZE_TIMEOUT: int = 30              # таймаут по умолчанию, сек
    ANALYZE_TIMEOUT: Optional[int] = None     # альтернативное имя переменной окружения

    # === Scrape params (используются в services/scrape.py) ===
    PARSE_MAX_CHUNK_SIZE: int = 100_000
    PARSE_MIN_HTML_LEN: int = 100
    PARSE_MAX_REDIRECTS: int = 3

    # Можно хранить CORS-настройки в .env (используются через getattr в app/main.py)
    CORS_ALLOW_ORIGINS: Optional[str] = None
    CORS_ALLOW_METHODS: Optional[str] = None
    CORS_ALLOW_HEADERS: Optional[str] = None
    CORS_ALLOW_CREDENTIALS: Optional[bool] = None

    # === Bitrix24 / batch / sync ===
    B24_BASE_URL: str | None = None
    # Реальный лимит элементов на странице у crm.company.list, по умолчанию 50
    B24_PAGE_LIMIT: int = 50
    # Совместимость со старым кодом (если где-то используется): не влияет на новую пагинацию
    B24_PAGE_SIZE: int = 200

    B24_BATCH_ENABLED: bool = True
    B24_BATCH_SIZE: int = 25               # будет принудительно ограничен 25 в коде клиента
    B24_BATCH_PAUSE_MS: int = 500          # пауза между batch-запросами (мс)

    B24_SYNC_ENABLED: bool = False
    B24_SYNC_INTERVAL: int = 600           # каждые 10 минут
    B24_SYNC_COMMIT_BATCH: int = 200       # коммит каждые N записей
    B24_SYNC_MAX_ITEMS: int | None = None  # лимит записей за один прогон (None/0 = без лимита)
    B24_SYNC_COMMIT_PAUSE_MS: int = 0      # пауза между commit-батчами (мс)

    # === Логирование Bitrix24 HTTP и шагов пагинации ===
    B24_LOG_VERBOSE: bool = False          # подробные шаги: start/next, страницы в батче, причины остановки
    B24_LOG_BODIES: bool = False           # логировать тела запросов/ответов (ОСТОРОЖНО: могут быть ПДн)
    B24_LOG_BODY_CHARS: int = 2000         # если включены тела — обрезать до N символов

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

    @property
    def embed_model(self) -> str:
        """Имя модели эмбеддингов для внешнего сервиса JSON-анализа."""
        return self.EMBED_MODEL or self.OPENAI_EMBED_MODEL


settings: Final[Settings] = Settings()
