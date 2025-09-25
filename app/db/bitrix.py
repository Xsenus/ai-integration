# app/db/bitrix.py
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator, Awaitable, Callable, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

log = logging.getLogger("db.bitrix")

_engine_bitrix: Optional[AsyncEngine] = None
_SessionBitrix: Optional[async_sessionmaker[AsyncSession]] = None


# ---------- Utilities ----------

async def run_with_retry(
    coro_factory: Callable[[], Awaitable[Any]],
    tries: int = 8,
    delay: float = 0.5,
) -> Any:
    """Запуск корутины с ретраями и линейной паузой."""
    last_exc: BaseException | None = None
    for i in range(tries):
        try:
            return await coro_factory()
        except BaseException as e:  # noqa: BLE001
            last_exc = e
            await asyncio.sleep(delay * (i + 1))
    raise RuntimeError(f"Operation failed after {tries} retries") from last_exc


async def ping_engine(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


# ---------- Bootstrap (NO-OP) ----------

async def ensure_app_role_exists() -> None:
    """NO-OP: роли/БД создаются вне приложения."""
    log.info("ensure_app_role_exists(): skipped")


async def ensure_bitrix_database_exists() -> None:
    """NO-OP: БД создаётся вне приложения."""
    log.info("ensure_bitrix_database_exists(): skipped")


# ---------- Engine / Session ----------

def get_bitrix_engine() -> Optional[AsyncEngine]:
    """
    Ленивая инициализация движка для БД bitrix_data.
    Если DSN не задан — возвращает None (мягкое отключение).
    """
    global _engine_bitrix
    url = settings.bitrix_url
    if not url:
        log.warning("BITRIX_DATABASE_URL не задан — соединение с bitrix_data отключено")
        return None
    if _engine_bitrix is None:
        _engine_bitrix = create_async_engine(
            url,
            pool_pre_ping=True,
            future=True,
            echo=settings.ECHO_SQL,
        )
    return _engine_bitrix


def get_bitrix_sessionmaker() -> Optional[async_sessionmaker[AsyncSession]]:
    """Фабрика сессий для bitrix_data или None, если DSN отсутствует."""
    global _SessionBitrix
    eng = get_bitrix_engine()
    if eng is None:
        return None
    if _SessionBitrix is None:
        _SessionBitrix = async_sessionmaker(eng, expire_on_commit=False)
    return _SessionBitrix


async def init_bitrix_engine() -> None:
    """Back-compat: прогреваем движок и фабрику сессий (не обязательно)."""
    get_bitrix_sessionmaker()


async def create_bitrix_tables(BaseBitrix: type[DeclarativeBase]) -> None:
    """Создать таблицы, если они описаны и движок активен."""
    eng = get_bitrix_engine()
    if eng is None:
        return
    async with eng.begin() as conn:
        await conn.run_sync(BaseBitrix.metadata.create_all)


# ---------- Session helpers ----------

async def get_bitrix_session() -> AsyncGenerator[AsyncSession, None]:
    """
    DI-зависимость для FastAPI — вернёт сессию или бросит, если DSN не задан.
    Использование:
        async def handler(session: AsyncSession = Depends(get_bitrix_session)):
            ...
    """
    sm = get_bitrix_sessionmaker()
    if sm is None:
        raise RuntimeError("bitrix_data недоступна: BITRIX_DATABASE_URL не задан")
    async with sm() as session:
        yield session


@asynccontextmanager
async def bitrix_session() -> AsyncIterator[AsyncSession]:
    """
    Удобный контекст-менеджер для ручного использования:
        async with bitrix_session() as s:
            await s.execute(...)
    Бросит исключение, если DSN не задан.
    """
    sm = get_bitrix_sessionmaker()
    if sm is None:
        raise RuntimeError("bitrix_data недоступна: BITRIX_DATABASE_URL не задан")
    async with sm() as session:
        yield session


@asynccontextmanager
async def try_bitrix_session() -> AsyncIterator[Optional[AsyncSession]]:
    """
    Мягкий контекст-менеджер: вернёт None, если DSN не задан.
        async with try_bitrix_session() as s:
            if s is None: ...
            else: await s.execute(...)
    """
    sm = get_bitrix_sessionmaker()
    if sm is None:
        yield None
        return
    async with sm() as session:
        yield session


async def ping_bitrix() -> bool:
    """Пинг соединения с bitrix_data (для /health)."""
    eng = get_bitrix_engine()
    if eng is None:
        return False
    try:
        async with eng.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False
