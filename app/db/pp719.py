# app/db/pp719.py
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import settings

log = logging.getLogger("db.pp719")

_engine_pp719: Optional[AsyncEngine] = None


def get_pp719_engine() -> Optional[AsyncEngine]:
    """
    Ленивая инициализация движка под БД pp719.
    Если DSN не задан — возвращает None (мягкое отключение).
    """
    global _engine_pp719
    url = settings.pp719_url
    if not url:
        log.warning("PP719_DATABASE_URL не задан — соединение с pp719 отключено")
        return None
    if _engine_pp719 is None:
        _engine_pp719 = create_async_engine(
            url, pool_pre_ping=True, future=True, echo=settings.ECHO_SQL
        )
    return _engine_pp719


async def pp719_has_inn(inn: str) -> bool:
    """
    true, если ИНН есть в pp719.public.product_clusters(inn).
    При отсутствии DSN/ошибке — False (без исключений).
    """
    eng = get_pp719_engine()
    if eng is None:
        return False
    try:
        sql = text("SELECT 1 FROM public.product_clusters WHERE inn = :inn LIMIT 1")
        async with eng.connect() as conn:
            return (await conn.execute(sql, {"inn": inn})).first() is not None
    except Exception as e:  # noqa: BLE001
        log.warning("pp719_has_inn failed for %s: %s", inn, e)
        return False
