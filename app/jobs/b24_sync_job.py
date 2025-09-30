from __future__ import annotations

import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.bitrix.b24_client import iter_companies, iter_companies_fast
from app.db.bitrix import bitrix_session
from app.config import settings
from app.repo.b24_company_repo import upsert_company

log = logging.getLogger(__name__)

async def sync_all_companies_once() -> dict:
    counters = {"updated": 0, "skipped": 0, "errors": 0}
    batch_size = int(getattr(settings, "B24_SYNC_COMMIT_BATCH", 100) or 100)
    i = 0

    async with bitrix_session() as session:
        async for comp in iter_companies_fast(all_props=True):
            try:
                status, affected = await upsert_company(session, comp)
                counters[status] = counters.get(status, 0) + affected
            except Exception as e:
                counters["errors"] += 1
                log.exception("upsert failed for ID=%s: %s", comp.get("ID"), e)

            i += 1
            if i % batch_size == 0:
                await session.commit()
                log.info("B24 sync: progress %s (batch committed)", i)
        await session.commit()

    log.info("B24 sync done: %s", counters)
    return counters

async def run_b24_sync_loop(interval_seconds: int = 600) -> None:
    while True:
        try:
            if not settings.B24_BASE_URL:
                log.warning("B24 sync skipped: B24_BASE_URL is not configured.")
            else:
                await sync_all_companies_once()
        except Exception as e:
            log.exception("B24 sync failed: %s", e)
        await asyncio.sleep(interval_seconds)
