from __future__ import annotations
from typing import Final
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Admin (bootstrap) для основного сервера с bitrix_data ---
    PG_BOOTSTRAP_USER: str | None = None
    PG_BOOTSTRAP_PASSWORD: str | None = None
    PG_BOOTSTRAP_DB: str | None = None

    # --- Runtime Postgres (основной сервер / bitrix_data) ---
    PG_HOST: str = "127.0.0.1"
    PG_PORT: int = 5432
    PG_USER: str = "dadata"
    PG_PASSWORD: str = "dadata"
    PG_ADMIN_DB: str = "postgres"

    BITRIX_DB_NAME: str = "bitrix_data"
    BITRIX_DATABASE_URL: str | None = None  # полная строка подключения (если задана — перекрывает поля выше)

    # --- ВТОРОЕ ПОДКЛЮЧЕНИЕ: сервер с parsing_data ---
    PARSING_PG_HOST: str = "127.0.0.1"
    PARSING_PG_PORT: int = 5432
    PARSING_PG_USER: str = "parsing"
    PARSING_PG_PASSWORD: str = "parsing"
    PARSING_DB_NAME: str = "parsing_data"
    PARSING_DATABASE_URL: str | None = None  # полная строка подключения (если задана — перекрывает поля выше)

    # --- DaData ---
    DADATA_API_KEY: str | None = None
    DADATA_SECRET_KEY: str | None = None

    # --- Scraper / Парсинг сайтов ---
    SCRAPERAPI_KEY: str | None = None         # ключ ScraperAPI
    PARSE_MAX_CHUNK_SIZE: int = 100_000       # жёсткий размер чанка
    PARSE_MIN_HTML_LEN: int = 100             # минимальная длина ответа HTML, иначе считаем "слишком короткий"

    # --- Misc ---
    ECHO_SQL: bool = False
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---------- Сформированные DSN/URL ----------
    @property
    def bitrix_url(self) -> str:
        """
        Строка подключения к основной БД (bitrix_data).
        Если BITRIX_DATABASE_URL задана — используется она, иначе собирается из атомарных полей.
        """
        return (
            self.BITRIX_DATABASE_URL
            or (
                f"postgresql+asyncpg://{self.PG_USER}:{self.PG_PASSWORD}@"
                f"{self.PG_HOST}:{self.PG_PORT}/{self.BITRIX_DB_NAME}"
            )
        )

    @property
    def bootstrap_dsn(self) -> str:
        """
        Синхронный DSN для bootstrap-операций (создание БД/пользователей и т.п.)
        """
        user = self.PG_BOOTSTRAP_USER or self.PG_USER
        pwd = self.PG_BOOTSTRAP_PASSWORD or self.PG_PASSWORD
        db = self.PG_BOOTSTRAP_DB or self.PG_ADMIN_DB
        return f"postgresql://{user}:{pwd}@{self.PG_HOST}:{self.PG_PORT}/{db}"

    @property
    def parsing_url(self) -> str:
        """
        Строка подключения ко второй БД (parsing_data).
        Если PARSING_DATABASE_URL задана — используется она, иначе собирается из атомарных полей.
        """
        return (
            self.PARSING_DATABASE_URL
            or (
                f"postgresql+asyncpg://{self.PARSING_PG_USER}:{self.PARSING_PG_PASSWORD}@"
                f"{self.PARSING_PG_HOST}:{self.PARSING_PG_PORT}/{self.PARSING_DB_NAME}"
            )
        )


settings: Final[Settings] = Settings()
