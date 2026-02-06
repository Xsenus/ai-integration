from __future__ import annotations

from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import bindparam, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from app.db.bitrix import get_bitrix_session
from app.db.parsing import get_parsing_engine
from app.models.bitrix import DaDataResult
from app.schemas.ai_analysis_status import (
    AiAnalysisCompaniesResponse,
    AiAnalysisCompanyStatus,
    AnalysisStatus,
)

log = logging.getLogger("api.ai-analysis")
router = APIRouter(prefix="/api/ai-analysis", tags=["ai-analysis"])

_STEPS_COUNT = 12

_LATEST_REQUESTS_SQL = (
    text(
        """
        WITH ranked AS (
            SELECT
                id,
                inn,
                company_name,
                created_at,
                started_at,
                ended_at,
                sec_duration,
                step_1, step_2, step_3, step_4, step_5, step_6,
                step_7, step_8, step_9, step_10, step_11, step_12,
                ROW_NUMBER() OVER (
                    PARTITION BY inn
                    ORDER BY COALESCE(ended_at, started_at, created_at) DESC NULLS LAST, id DESC
                ) AS rn
            FROM public.clients_requests
            WHERE inn IN :inns
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        """
    ).bindparams(bindparam("inns", expanding=True))
)


def _clamp_progress(value: float) -> float:
    return max(0.0, min(1.0, value))


def _ms_between(later: datetime | None, earlier: datetime | None) -> int:
    if later is None or earlier is None:
        return 0
    return max(0, int((later - earlier).total_seconds() * 1000))


def _resolve_status(row: dict[str, object]) -> AnalysisStatus:
    ended_at = row.get("ended_at")
    started_at = row.get("started_at")
    has_any_step = any(bool(row.get(f"step_{idx}")) for idx in range(1, _STEPS_COUNT + 1))

    if ended_at is not None:
        if bool(row.get("step_12")):
            return "completed"
        if started_at is not None or has_any_step:
            return "failed"
        return "stopped"
    if started_at is not None or has_any_step:
        return "running"
    if row.get("created_at") is not None:
        return "queued"
    return "stopped"


def _resolve_progress(row: dict[str, object], status: AnalysisStatus) -> float | None:
    if status == "stopped":
        return None

    done = sum(1 for idx in range(1, _STEPS_COUNT + 1) if bool(row.get(f"step_{idx}")))
    if status == "completed":
        return 1.0

    if done <= 0:
        if status == "queued":
            return 0.0
        return 0.01

    base = done / _STEPS_COUNT
    if status in {"running", "queued", "failed"}:
        return _clamp_progress(min(base, 0.99))
    return _clamp_progress(base)


def _resolve_duration_ms(row: dict[str, object], status: AnalysisStatus, now_utc: datetime) -> int:
    started_at = row.get("started_at")
    created_at = row.get("created_at")
    ended_at = row.get("ended_at")

    sec_duration = row.get("sec_duration")
    sec_duration_ms = 0
    if sec_duration is not None:
        try:
            sec_duration_ms = max(0, int(sec_duration) * 1000)
        except (TypeError, ValueError):
            sec_duration_ms = 0

    timeline_ms = 0
    if status in {"completed", "failed", "stopped"}:
        timeline_ms = _ms_between(ended_at, started_at) or _ms_between(ended_at, created_at)
    elif status == "running":
        timeline_ms = _ms_between(now_utc, started_at) or _ms_between(now_utc, created_at)
    elif status == "queued":
        timeline_ms = _ms_between(now_utc, created_at)

    return max(sec_duration_ms, timeline_ms)


async def _fetch_latest_requests(conn: AsyncConnection, inns: list[str]) -> dict[str, dict[str, object]]:
    if not inns:
        return {}

    result = await conn.execute(_LATEST_REQUESTS_SQL, {"inns": inns})
    rows: dict[str, dict[str, object]] = {}
    for row in result.mappings():
        inn = str(row.get("inn") or "").strip()
        if not inn:
            continue
        rows[inn] = dict(row)
    return rows


@router.get("/companies", response_model=AiAnalysisCompaniesResponse)
async def get_ai_analysis_companies(
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_bitrix_session),
) -> AiAnalysisCompaniesResponse:
    parsing_engine: AsyncEngine | None = get_parsing_engine()
    if parsing_engine is None:
        raise HTTPException(status_code=503, detail="Parsing DB is not configured")

    stmt = (
        select(DaDataResult.inn, DaDataResult.short_name)
        .where(DaDataResult.inn.is_not(None))
        .order_by(DaDataResult.updated_at.desc(), DaDataResult.inn.asc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    companies = [(str(inn).strip(), short_name) for inn, short_name in result.all() if str(inn or "").strip()]

    inns = [inn for inn, _ in companies]

    async with parsing_engine.connect() as conn:
        latest_by_inn = await _fetch_latest_requests(conn, inns)

    now_utc = datetime.now(timezone.utc)
    items: list[AiAnalysisCompanyStatus] = []
    for inn, short_name in companies:
        req = latest_by_inn.get(inn)
        if req is None:
            items.append(
                AiAnalysisCompanyStatus(
                    inn=inn,
                    company_name=short_name,
                    analysis_status="stopped",
                    queued_at=None,
                    analysis_started_at=None,
                    analysis_finished_at=None,
                    analysis_duration_ms=0,
                    analysis_progress=None,
                    run_id=None,
                )
            )
            continue

        status = _resolve_status(req)
        items.append(
            AiAnalysisCompanyStatus(
                inn=inn,
                company_name=(req.get("company_name") or short_name),
                analysis_status=status,
                queued_at=req.get("created_at"),
                analysis_started_at=req.get("started_at"),
                analysis_finished_at=req.get("ended_at") if status in {"completed", "failed", "stopped"} else None,
                analysis_duration_ms=_resolve_duration_ms(req, status, now_utc),
                analysis_progress=_resolve_progress(req, status),
                run_id=req.get("id"),
            )
        )

    return AiAnalysisCompaniesResponse(items=items, generated_at=now_utc)
