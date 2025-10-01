from __future__ import annotations

import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.bitrix.b24_client import iter_companies_fast
from app.db.bitrix import bitrix_session
from app.config import settings
from app.repo.b24_company_repo import upsert_company

log = logging.getLogger(__name__)


async def sync_all_companies_once() -> dict:
    """
    Тянет все компании из B24 и апсеррит их в bitrix_data.
    Коммитим пакетами, чтобы данные были видны быстро и не терялись при сбоях.
    Учитываем ручной лимит записей за один прогон (B24_SYNC_MAX_ITEMS).
    """
    counters = {"updated": 0, "skipped": 0, "errors": 0, "read": 0}
    batch_size = int(getattr(settings, "B24_SYNC_COMMIT_BATCH", 200) or 200)
    commit_pause_ms = int(getattr(settings, "B24_SYNC_COMMIT_PAUSE_MS", 0) or 0)
    max_items = getattr(settings, "B24_SYNC_MAX_ITEMS", None)
    stopped_by_limit = False
    i = 0

    async with bitrix_session() as session:
        async for comp in iter_companies_fast(all_props=True, max_items=max_items):
            try:
                status, affected = await upsert_company(session, comp)
                counters[status] = counters.get(status, 0) + affected
                counters["read"] += 1
            except Exception as e:
                counters["errors"] += 1
                log.exception("upsert failed for ID=%s: %s", comp.get("ID"), e)

            i += 1
            if i % batch_size == 0:
                await session.commit()
                log.info(
                    "B24 sync: progress %s (batch committed) [updated=%s skipped=%s errors=%s read=%s]",
                    i, counters["updated"], counters["skipped"], counters["errors"], counters["read"]
                )
                # Небольшая пауза между коммит-батчами (если задана)
                if commit_pause_ms > 0:
                    try:
                        await asyncio.sleep(commit_pause_ms / 1000.0)
                    except Exception:
                        pass

        await session.commit()

    if max_items and counters["read"] >= max_items:
        stopped_by_limit = True
    log.info("B24 sync done: %s%s", counters, " (stopped_by_limit)" if stopped_by_limit else "")
    return counters


async def run_b24_sync_loop(interval_seconds: int = 600) -> None:
    while True:
        try:
            # Возможность отключить задачу
            if not getattr(settings, "B24_SYNC_ENABLED", True):
                log.info("B24 sync skipped: disabled by B24_SYNC_ENABLED.")
            elif not settings.B24_BASE_URL:
                log.warning("B24 sync skipped: B24_BASE_URL is not configured.")
            else:
                await sync_all_companies_once()
        except Exception as e:
            log.exception("B24 sync failed: %s", e)
        await asyncio.sleep(interval_seconds)
