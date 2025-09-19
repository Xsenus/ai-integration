# app/db/bitrix.py
from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator, Callable, Awaitable, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from ..config import settings

log = logging.getLogger(__name__)

engine_bitrix: AsyncEngine | None = None
SessionBitrix: async_sessionmaker[AsyncSession] | None = None


# ---------- Utilities ----------

async def run_with_retry(
    coro_factory: Callable[[], Awaitable[Any]],
    tries: int = 8,
    delay: float = 0.5,
) -> Any:
    """
    Запускает корутину с ретраями. Между попытками — линейная задержка.
    """
    last_exc: BaseException | None = None
    for i in range(tries):
        try:
            return await coro_factory()
        except BaseException as e:
            last_exc = e
            await asyncio.sleep(delay * (i + 1))
    raise RuntimeError(f"Operation failed after {tries} retries") from last_exc


async def ping_engine(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def wait_for_postgres(retries: int = 30, delay: float = 0.5) -> None:
    """
    Ждём доступности БД через SQLAlchemy по settings.bitrix_url.
    Bootstrap-dsn/asyncpg не используются.
    """
    if not settings.bitrix_url:
        raise RuntimeError("BITRIX_DATABASE_URL is not set")

    engine = create_async_engine(settings.bitrix_url, pool_pre_ping=True, future=True)
    last_err: BaseException | None = None

    try:
        for _ in range(retries):
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                return
            except BaseException as e:
                last_err = e
                await asyncio.sleep(delay)
        raise RuntimeError(f"Postgres is not reachable after {retries} retries") from last_err
    finally:
        # гарантированно освобождаем ресурсы
        await engine.dispose()


# ---------- Bootstrap (no-op в DSN-режиме) ----------

async def ensure_app_role_exists() -> None:
    """
    NO-OP: в DSN-режиме роли/пользователи создаются вне приложения.
    Оставлено для обратной совместимости.
    """
    log.info("ensure_app_role_exists(): skipped (DSN-only mode)")


async def ensure_bitrix_database_exists() -> None:
    """
    NO-OP: в DSN-режиме БД создаётся вне приложения.
    Оставлено для обратной совместимости.
    """
    log.info("ensure_bitrix_database_exists(): skipped (DSN-only mode)")


# ---------- Engine / Session ----------

async def init_bitrix_engine() -> None:
    """Инициализация движка и фабрики сессий для bitrix_data."""
    if not settings.bitrix_url:
        raise RuntimeError("BITRIX_DATABASE_URL is not set")
    global engine_bitrix, SessionBitrix
    engine_bitrix = create_async_engine(
        settings.bitrix_url,
        echo=getattr(settings, "ECHO_SQL", False),
        pool_pre_ping=True,
        future=True,
    )
    SessionBitrix = async_sessionmaker(engine_bitrix, expire_on_commit=False)


async def create_bitrix_tables(BaseBitrix: type[DeclarativeBase]) -> None:
    """Создать таблицы, если их нет."""
    if engine_bitrix is None:
        raise RuntimeError("Bitrix engine is not initialized")
    async with engine_bitrix.begin() as conn:
        await conn.run_sync(BaseBitrix.metadata.create_all)


async def get_bitrix_session() -> AsyncGenerator[AsyncSession, None]:
    """DI-зависимость для FastAPI."""
    if SessionBitrix is None:
        raise RuntimeError("Bitrix session factory is not initialized")
    async with SessionBitrix() as session:
        yield session


async def ping_bitrix() -> bool:
    """Лёгкая проверка соединения с текущим engine (для /health)."""
    if engine_bitrix is None:
        return False
    try:
        async with engine_bitrix.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
