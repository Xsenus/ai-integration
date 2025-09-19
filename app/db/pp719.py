# app/db/pp719.py
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import settings

log = logging.getLogger("dadata-bitrix")

_engine_pp719: Optional[AsyncEngine] = None


def get_pp719_engine() -> Optional[AsyncEngine]:
    """Ленивая инициализация движка под БД pp719. Если URL не задан — вернём None."""
    global _engine_pp719
    url = settings.pp719_url
    if not url:
        return None
    if _engine_pp719 is None:
        _engine_pp719 = create_async_engine(url, pool_pre_ping=True, future=True)
    return _engine_pp719


async def pp719_has_inn(inn: str) -> bool:
    """
    true, если ИНН есть в pp719.public.companies(inn).
    Если URL не задан или ошибка подключения — безопасно возвращаем False и логируем.
    """
    try:
        eng = get_pp719_engine()
        if eng is None:
            return False
        sql = text("SELECT 1 FROM public.companies WHERE inn = :inn LIMIT 1")
        async with eng.connect() as conn:
            row = (await conn.execute(sql, {"inn": inn})).first()
            return row is not None
    except Exception as e:
        log.warning("pp719_has_inn failed for %s: %s", inn, e)
        return False
