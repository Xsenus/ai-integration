from __future__ import annotations

from datetime import datetime, timezone
import logging
from decimal import Decimal
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import bindparam, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from app.config import settings
from app.db.bitrix import get_bitrix_session
from app.db.parsing import get_parsing_engine
from app.db.postgres import get_postgres_engine
from app.models.bitrix import DaDataResult
from app.schemas.ai_analysis_status import (
    AiAnalysisCompaniesResponse,
    AiAnalysisCompanyStatus,
    AnalysisStatus,
)
from app.services.analyze_client import get_analyze_base_url

log = logging.getLogger("api.ai-analysis")
router = APIRouter(prefix="/api/ai-analysis", tags=["ai-analysis"])
public_router = APIRouter(prefix="/api", tags=["ai-analysis"])

_STEPS_COUNT = 12
_BILLING_REMAINING_PATH = "/v1/billing/remaining"

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
        # В очереди задача ещё не исполняется, поэтому не наращиваем
        # длительность «живым» таймером от времени создания.
        timeline_ms = 0

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


async def _fetch_latest_requests_by_ids(
    conn: AsyncConnection, company_ids: list[int]
) -> dict[int, dict[str, Any]]:
    if not company_ids:
        return {}

    stmt = (
        text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    started_at,
                    ended_at,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY COALESCE(ended_at, started_at, created_at) DESC NULLS LAST
                    ) AS rn
                FROM public.clients_requests
                WHERE id IN :company_ids
            )
            SELECT id, started_at, ended_at, created_at
            FROM ranked
            WHERE rn = 1
            """
        ).bindparams(bindparam("company_ids", expanding=True))
    )
    result = await conn.execute(stmt, {"company_ids": company_ids})
    rows: dict[int, dict[str, Any]] = {}
    for row in result.mappings():
        row_id = row.get("id")
        if row_id is None:
            continue
        rows[int(row_id)] = dict(row)
    return rows


async def _fetch_company_cost_rows(
    conn: AsyncConnection,
    company_id: int,
    *,
    started_at: datetime | None,
    finished_at: datetime | None,
) -> list[dict[str, Any]]:
    if started_at is not None and finished_at is not None and finished_at >= started_at:
        result = await conn.execute(
            text(
                """
                SELECT model, input_tokens, cached_input_tokens, output_tokens, cost_usd
                FROM public.ai_site_openai_responses
                WHERE company_id = :company_id
                  AND created_at >= :started_at
                  AND created_at <= :finished_at
                """
            ),
            {
                "company_id": company_id,
                "started_at": started_at,
                "finished_at": finished_at,
            },
        )
        return [dict(row) for row in result.mappings()]

    lookback_hours = max(1, int(settings.AI_ANALYSIS_COSTS_LOOKBACK_HOURS or 24))
    result = await conn.execute(
        text(
            """
            SELECT model, input_tokens, cached_input_tokens, output_tokens, cost_usd
            FROM public.ai_site_openai_responses
            WHERE company_id = :company_id
              AND created_at >= now() - make_interval(hours => :lookback_hours)
            """
        ),
        {
            "company_id": company_id,
            "lookback_hours": lookback_hours,
        },
    )
    return [dict(row) for row in result.mappings()]


def _aggregate_cost_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tokens_input = 0
    tokens_cached_input = 0
    tokens_output = 0
    cost_total = Decimal("0")
    breakdown: dict[str, Decimal] = {}

    for row in rows:
        tokens_input += int(row.get("input_tokens") or 0)
        tokens_cached_input += int(row.get("cached_input_tokens") or 0)
        tokens_output += int(row.get("output_tokens") or 0)

        raw_cost = row.get("cost_usd")
        cost = Decimal(str(raw_cost or 0))
        if cost > 0:
            cost_total += cost
            model_name = str(row.get("model") or "unknown")
            breakdown[model_name] = breakdown.get(model_name, Decimal("0")) + cost

    tokens_total = tokens_input + tokens_cached_input + tokens_output
    breakdown_out = {k: float(v) for k, v in sorted(breakdown.items())} if breakdown else None
    return {
        "tokens_input": tokens_input,
        "tokens_cached_input": tokens_cached_input,
        "tokens_output": tokens_output,
        "tokens_total": tokens_total,
        "cost_total_usd": float(cost_total),
        "breakdown": breakdown_out,
    }


async def _fetch_billing_remaining() -> dict[str, Any]:
    base_url = get_analyze_base_url()
    if not base_url:
        raise HTTPException(status_code=503, detail="ANALYZE_BASE is not configured")

    headers: dict[str, str] = {}
    admin_key = (settings.analyze_admin_key or "").strip()
    if admin_key:
        headers["X-Admin-Key"] = admin_key

    url = f"{base_url.rstrip('/')}{_BILLING_REMAINING_PATH}"
    timeout = max(1, int(settings.analyze_timeout or 15))
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.get(url, headers=headers)
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Analyze service request failed: {exc}") from exc

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Analyze service returned {response.status_code}",
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Analyze service returned non-JSON response") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Analyze service returned invalid billing payload")
    return data


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

    postgres_engine = get_postgres_engine()
    costs_by_company_id: dict[int, dict[str, Any]] = {}
    if postgres_engine is not None:
        company_ids = [int(req["id"]) for req in latest_by_inn.values() if req.get("id") is not None]
        async with parsing_engine.connect() as parsing_conn:
            latest_by_id = await _fetch_latest_requests_by_ids(parsing_conn, company_ids)
        async with postgres_engine.connect() as conn:
            for company_id, latest in latest_by_id.items():
                rows = await _fetch_company_cost_rows(
                    conn,
                    company_id,
                    started_at=latest.get("started_at"),
                    finished_at=latest.get("ended_at"),
                )
                costs_by_company_id[company_id] = _aggregate_cost_rows(rows)

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
                    tokens_total=0,
                    cost_total_usd=0.0,
                    tokens_input=0,
                    tokens_cached_input=0,
                    tokens_output=0,
                    breakdown=None,
                )
            )
            continue

        status = _resolve_status(req)
        company_id = int(req.get("id")) if req.get("id") is not None else None
        cost_data = (
            costs_by_company_id.get(company_id, _aggregate_cost_rows([]))
            if company_id is not None
            else _aggregate_cost_rows([])
        )
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
                tokens_total=cost_data["tokens_total"],
                cost_total_usd=cost_data["cost_total_usd"],
                tokens_input=cost_data["tokens_input"],
                tokens_cached_input=cost_data["tokens_cached_input"],
                tokens_output=cost_data["tokens_output"],
                breakdown=cost_data["breakdown"],
            )
        )

    return AiAnalysisCompaniesResponse(items=items, generated_at=now_utc)


@public_router.get("/billing/remaining")
async def get_billing_remaining() -> dict[str, Any]:
    return await _fetch_billing_remaining()


@public_router.get("/companies/{company_id}/costs")
async def get_company_costs(company_id: int) -> dict[str, Any]:
    parsing_engine = get_parsing_engine()
    postgres_engine = get_postgres_engine()
    if parsing_engine is None or postgres_engine is None:
        raise HTTPException(status_code=503, detail="Required databases are not configured")

    async with parsing_engine.connect() as conn:
        latest = (await _fetch_latest_requests_by_ids(conn, [company_id])).get(company_id)

    started_at = latest.get("started_at") if latest else None
    finished_at = latest.get("ended_at") if latest else None
    created_at = latest.get("created_at") if latest else None

    async with postgres_engine.connect() as conn:
        rows = await _fetch_company_cost_rows(
            conn,
            company_id,
            started_at=started_at,
            finished_at=finished_at,
        )

    aggregate = _aggregate_cost_rows(rows)
    return {
        "company_id": company_id,
        "analysis_started_at": started_at,
        "analysis_finished_at": finished_at,
        "analysis_created_at": created_at,
        **aggregate,
    }
