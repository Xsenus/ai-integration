# app/main.py
from __future__ import annotations

import logging
from typing import cast

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import settings
from app.api.routes import router as api_router
from app.models.bitrix import BaseBitrix
import app.db.bitrix as db 
from starlette.middleware.cors import CORSMiddleware

# --- Logging ---
LOG_LEVEL = (settings.LOG_LEVEL or "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dadata-bitrix")

# --- FastAPI app ---
app = FastAPI(title="DaData Bitrix Service", version="1.3.1")

# CORS
origins = [o.strip() for o in (settings.CORS_ALLOW_ORIGINS or "").split(",") if o.strip()]
if origins:
    methods = [m.strip() for m in (settings.CORS_ALLOW_METHODS or "").split(",") if m.strip()]
    headers = [h.strip() for h in (settings.CORS_ALLOW_HEADERS or "").split(",") if h.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=methods or ["*"],
        allow_headers=headers or ["*"],
        allow_credentials=bool(settings.CORS_ALLOW_CREDENTIALS),
    )

app.include_router(api_router)  # подключаем /v1/... маршруты

@app.on_event("startup")
async def startup() -> None:
    # 0) ждём доступности Postgres (устойчиво к гонкам Windows/Docker)
    await db.wait_for_postgres(retries=40, delay=0.5)

    # 1) гарантируем наличие роли приложения
    await db.run_with_retry(lambda: db.ensure_app_role_exists(), tries=8, delay=0.5)

    # 2) гарантируем наличие БД и владельца
    await db.run_with_retry(lambda: db.ensure_bitrix_database_exists(), tries=8, delay=0.5)

    # 3) инициализируем engine и проверяем коннект
    await db.init_bitrix_engine()
    if db.engine_bitrix is None:
        raise RuntimeError("Bitrix engine was not initialized")

    eng: AsyncEngine = cast(AsyncEngine, db.engine_bitrix)
    await db.run_with_retry(lambda: db.ping_engine(eng), tries=8, delay=0.5)

    # 4) создаём таблицы (idempotent)
    await db.create_bitrix_tables(BaseBitrix)
    log.info("Инициализация завершена. URL: %s", settings.bitrix_url)


@app.on_event("shutdown")
async def shutdown() -> None:
    if db.engine_bitrix is not None:
        await db.engine_bitrix.dispose()
        log.info("Соединение с БД закрыто.")


@app.get("/health")
async def health():
    """Простой healthcheck + быстрый SQL ping."""
    try:
        if db.engine_bitrix is None:
            return {"ok": False, "error": "engine not initialized"}
        eng: AsyncEngine = cast(AsyncEngine, db.engine_bitrix)
        async with eng.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"ok": True}
    except Exception as e:
        log.exception("health failed: %s", e)
        return {"ok": False, "error": str(e)}    
