from __future__ import annotations

import asyncio
import logging
from typing import List

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.middleware.cors import CORSMiddleware

from app.config import settings
from app.api.routes import router as api_router
from app.api.lookup import router as lookup_router
from app.api.analyze_json import (
    router as analyze_json_router,
    close_analyze_json_http_client,
)
from app.api.pipeline import router as pipeline_router
from app.api.ai_analysis import router as ai_analysis_router
from app.api.ai_analysis import public_router as ai_analysis_public_router
from app.api.billing import router as billing_router

# DB helpers
from app.db.bitrix import (
    get_bitrix_engine,
    ping_bitrix,
    create_bitrix_tables,
)
from app.db.parsing import get_parsing_engine, ensure_parsing_schema
from app.db.pp719 import get_pp719_engine
from app.db.postgres import get_postgres_engine, ping_postgres

# Bitrix24 raw companies (модель таблицы) + sync job
from app.models.bitrix_company import BaseBitrix
from app.jobs.b24_sync_job import run_b24_sync_loop
from app.services.analyze_client import close_analyze_clients

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
app.include_router(lookup_router)
app.include_router(analyze_json_router)
app.include_router(pipeline_router)
app.include_router(ai_analysis_router)
app.include_router(ai_analysis_public_router)
app.include_router(billing_router)

# Хэндлы фоновых задач (для корректной остановки)
_bg_tasks: List[asyncio.Task] = []


def _dsn_configured(value: str | None) -> bool:
    return bool(value and value.strip())


async def _probe_connection(
    *,
    dsn_configured: bool,
    ping_fn=None,
    engine_getter=None,
    logger_name: str,
) -> str:
    if not dsn_configured:
        return "disabled"

    try:
        if ping_fn is not None:
            return "ok" if await ping_fn() else "down"

        eng = engine_getter() if engine_getter is not None else None
        if eng is None:
            return "down"
        async with eng.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(logger_name).info("healthcheck ping failed: %s", exc)
        return "down"


def _health_ok(connections: dict[str, str]) -> bool:
    enabled = [status for status in connections.values() if status != "disabled"]
    return all(status == "ok" for status in enabled)


@app.on_event("startup")
async def on_startup() -> None:
    # Инициализируем коннекторы (если DSN заданы)
    bitrix_eng = get_bitrix_engine()
    get_parsing_engine()
    get_pp719_engine()
    get_postgres_engine()

    # Создаём/проверяем схему parsing_data
    await ensure_parsing_schema()

    try:
        # Создаст таблицу b24_companies_raw, если её раньше не было
        await create_bitrix_tables(BaseBitrix)
        log.info("bitrix_data: tables ensured (b24_companies_raw).")
    except Exception:
        log.exception("bitrix_data: failed to ensure tables")

    try:
        bitrix_eng = get_bitrix_engine()
        if settings.B24_SYNC_ENABLED and bitrix_eng is not None and settings.B24_BASE_URL:
            interval = int(settings.B24_SYNC_INTERVAL or 600)
            t = asyncio.create_task(run_b24_sync_loop(interval))
            _bg_tasks.append(t)
            log.info("B24 sync loop started (interval=%ss).", interval)
        else:
            if not settings.B24_SYNC_ENABLED:
                log.info("B24 sync loop disabled by settings.")
            elif not settings.B24_BASE_URL:
                log.warning("B24 sync loop NOT started: B24_BASE_URL is not configured.")
            else:
                log.warning("B24 sync loop NOT started: bitrix_data DSN is empty.")
    except Exception:
        log.exception("Failed to start B24 sync loop")

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
    # Корректно останавливаем фоновые задачи
    for t in _bg_tasks:
        try:
            t.cancel()
        except Exception:
            pass
    # Дадим задачам шанс отмениться
    if _bg_tasks:
        try:
            await asyncio.gather(*_bg_tasks, return_exceptions=True)
        except Exception:
            pass
    _bg_tasks.clear()

    # Закрываем HTTP-клиенты внешних сервисов
    try:
        await close_analyze_json_http_client()
    except Exception:
        log.exception("Failed to close analyze-json HTTP client")
    try:
        await close_analyze_clients()
    except Exception:
        log.exception("Failed to close analyze HTTP clients")

    # Закрываем коннекты к БД
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
    connections = {
        "bitrix_data": await _probe_connection(
            dsn_configured=_dsn_configured(settings.bitrix_url),
            ping_fn=ping_bitrix,
            logger_name="db.bitrix",
        ),
        "parsing_data": await _probe_connection(
            dsn_configured=_dsn_configured(settings.parsing_url),
            engine_getter=get_parsing_engine,
            logger_name="db.parsing",
        ),
        "pp719": await _probe_connection(
            dsn_configured=_dsn_configured(settings.pp719_url),
            engine_getter=get_pp719_engine,
            logger_name="db.pp719",
        ),
        "postgres": await _probe_connection(
            dsn_configured=_dsn_configured(settings.postgres_url),
            ping_fn=ping_postgres,
            logger_name="db.postgres",
        ),
    }

    return {"ok": _health_ok(connections), "connections": connections}
