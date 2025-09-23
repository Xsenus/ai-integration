# app/db/postgres.py
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import settings

log = logging.getLogger("db.postgres")

_engine_postgres: Optional[AsyncEngine] = None


def get_postgres_engine() -> Optional[AsyncEngine]:
    """
    Ленивая инициализация движка для БД postgres.
    Если DSN не задан — возвращает None (мягкое отключение).
    """
    global _engine_postgres
    url = settings.postgres_url
    if not url:
        log.warning("POSTGRES_DATABASE_URL не задан — соединение с postgres отключено")
        return None
    if _engine_postgres is None:
        _engine_postgres = create_async_engine(
            url, pool_pre_ping=True, future=True, echo=settings.ECHO_SQL
        )
    return _engine_postgres


async def ping_postgres() -> bool:
    """Пинг соединения с postgres (для /health)."""
    eng = get_postgres_engine()
    if eng is None:
        return False
    try:
        async with eng.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as e:  # noqa: BLE001
        log.error("postgres ping failed: %s", e)
        return False
