# app/db/bitrix.py
from __future__ import annotations

import asyncio
from typing import AsyncGenerator, Callable, Awaitable, Any, Optional

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from ..config import settings

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
    last_exc: Optional[BaseException] = None
    for i in range(tries):
        try:
            return await coro_factory()
        except BaseException as e:
            last_exc = e
            await asyncio.sleep(delay * (i + 1))
    # Никогда не поднимаем "None" как исключение
    raise RuntimeError(f"Operation failed after {tries} retries") from last_exc


async def ping_engine(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


def quote_literal(val: str) -> str:
    """
    Экранирует строку как SQL-литерал для Postgres.
    Превращает одинарные кавычки в двойные: O'Neil -> 'O''Neil'
    """
    if not isinstance(val, str):
        raise TypeError("literal must be str")
    if "\x00" in val:
        raise ValueError("NUL byte is not allowed in literals")
    return "'" + val.replace("'", "''") + "'"


def quote_ident(name: str) -> str:
    """
    Безопасное квотирование идентификатора Postgres:
    - экранирует двойные кавычки,
    - оборачивает в двойные кавычки,
    - запрещает NUL-байт.
    """
    if not isinstance(name, str):
        raise TypeError("identifier must be str")
    if "\x00" in name:
        raise ValueError("NUL byte is not allowed in identifiers")
    return '"' + name.replace('"', '""') + '"'


async def wait_for_postgres(retries: int = 30, delay: float = 0.5) -> None:
    """Ждём, пока админ-подключение начнёт проходить (устойчиво к старту службы)."""
    last_err: Optional[BaseException] = None
    for _ in range(retries):
        try:
            conn = await asyncpg.connect(settings.bootstrap_dsn, timeout=3)
            await conn.close()
            return
        except BaseException as e:
            last_err = e
            await asyncio.sleep(delay)
    raise RuntimeError(f"Postgres is not reachable after {retries} retries") from last_err


# ---------- Bootstrap (role + database) ----------

async def ensure_app_role_exists() -> None:
    """
    Создаёт/обновляет роль приложения (PG_USER) через bootstrap-подключение.
    PASSWORD в DDL нельзя параметризовать — подставляем как литерал.
    """
    conn = await asyncpg.connect(settings.bootstrap_dsn)
    try:
        role_exists = await conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1",
            settings.PG_USER,
        )

        role_ident = quote_ident(settings.PG_USER)
        pwd_lit = quote_literal(settings.PG_PASSWORD)

        if not role_exists:
            await conn.execute(f"CREATE ROLE {role_ident} WITH LOGIN PASSWORD {pwd_lit}")
        else:
            await conn.execute(f"ALTER ROLE {role_ident} WITH PASSWORD {pwd_lit}")
    finally:
        await conn.close()


async def ensure_bitrix_database_exists() -> None:
    """
    Создаёт БД BITRIX_DB_NAME (владелец PG_USER) через bootstrap-подключение.
    """
    conn = await asyncpg.connect(settings.bootstrap_dsn)
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            settings.BITRIX_DB_NAME,
        )
        if not exists:
            role_ident = quote_ident(settings.PG_USER)
            db_ident = quote_ident(settings.BITRIX_DB_NAME)
            await conn.execute(f"CREATE DATABASE {db_ident} OWNER {role_ident}")
    finally:
        await conn.close()


# ---------- Engine / Session ----------

async def init_bitrix_engine() -> None:
    """Инициализация движка и фабрики сессий для bitrix_data."""
    global engine_bitrix, SessionBitrix
    engine_bitrix = create_async_engine(
        settings.bitrix_url,
        echo=settings.ECHO_SQL,
        pool_pre_ping=True,
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
