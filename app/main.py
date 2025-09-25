from __future__ import annotations

import logging
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.middleware.cors import CORSMiddleware

from app.config import settings
from app.api.routes import router as api_router
from app.api.ai_analyzer import router as ai_analyzer_router

# DB helpers
from app.db.bitrix import get_bitrix_engine, ping_bitrix
from app.db.parsing import get_parsing_engine, ensure_parsing_schema
from app.db.pp719 import get_pp719_engine
from app.db.postgres import get_postgres_engine, ping_postgres

# --- Logging ---
LOG_LEVEL = (settings.LOG_LEVEL or "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ai-integration")

# --- FastAPI app ---
app = FastAPI(title="ai-integration API", version="1.0.0")

# --- CORS (из .env) ---
origins = [o.strip() for o in (getattr(settings, "CORS_ALLOW_ORIGINS", "") or "").split(",") if o.strip()]
if origins:
    methods = [m.strip() for m in (getattr(settings, "CORS_ALLOW_METHODS", "") or "").split(",") if m.strip()]
    headers = [h.strip() for h in (getattr(settings, "CORS_ALLOW_HEADERS", "") or "").split(",") if h.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=methods or ["*"],
        allow_headers=headers or ["*"],
        allow_credentials=bool(getattr(settings, "CORS_ALLOW_CREDENTIALS", False)),
    )

# --- Routers ---
app.include_router(api_router)
app.include_router(ai_analyzer_router)

@app.on_event("startup")
async def on_startup() -> None:
    # Инициализируем коннекторы (если DSN заданы)
    get_bitrix_engine()
    get_parsing_engine()
    get_pp719_engine()
    get_postgres_engine()

    # Создаём/проверяем схему parsing_data
    await ensure_parsing_schema()

    # Пролог маршрутов (для быстрой самопроверки)
    try:
        routes_dump = []
        for r in app.router.routes:
            path = getattr(r, "path", "")
            methods = sorted(getattr(r, "methods", []) or [])
            if path:
                routes_dump.append(f"{path} [{', '.join(methods)}]")
        log.info("Registered routes:\n" + "\n".join(routes_dump))
    except Exception as e:
        log.debug("Route dump failed: %s", e)

    log.info("Startup complete: engines initialized (where DSN provided).")

@app.on_event("shutdown")
async def on_shutdown() -> None:
    engines: list[AsyncEngine | None] = [
        get_bitrix_engine(),
        get_parsing_engine(),
        get_pp719_engine(),
        get_postgres_engine(),
    ]
    for eng in engines:
        if eng is not None:
            await eng.dispose()
    log.info("All database engines disposed.")

@app.get("/health")
async def health():
    """
    Healthcheck пингует все четыре базы.
    ok=true только если доступны все, для которых заданы DSN.
    """
    results = {
        "bitrix_data": await ping_bitrix(),
        "parsing_data": False,
        "pp719": False,
        "postgres": await ping_postgres(),
    }

    # parsing_data ping
    try:
        eng = get_parsing_engine()
        if eng is not None:
            async with eng.connect() as conn:
                await conn.execute(text("SELECT 1"))
            results["parsing_data"] = True
    except Exception as e:
        logging.getLogger("db.parsing").warning("parsing ping failed: %s", e)

    # pp719 ping
    try:
        eng = get_pp719_engine()
        if eng is not None:
            async with eng.connect() as conn:
                await conn.execute(text("SELECT 1"))
            results["pp719"] = True
    except Exception as e:
        logging.getLogger("db.pp719").warning("pp719 ping failed: %s", e)

    ok = all(results.values()) if results else False
    return {"ok": ok, "connections": results}
