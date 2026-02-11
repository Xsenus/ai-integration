from __future__ import annotations

import logging
import time
from typing import Awaitable, Callable, Iterable

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import bindparam, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from app.api.analyze_json import analyze_from_inn_get
from app.api.lookup import lookup_card_get
from app.api.routes import (
    get_equipment_selection_by_inn,
    ib_match_by_inn_get,
)
from app.db.bitrix import get_bitrix_session
from app.db.postgres import get_postgres_engine
from app.models.bitrix import DaDataResult
from app.schemas.pipeline import (
    PipelineAutoCandidate,
    PipelineAutoResponse,
    PipelineAutoResult,
    PipelineFullRequest,
    PipelineFullResponse,
    PipelineStepError,
)
from app.services.parse_site import ParseSiteRequest, run_parse_site

log = logging.getLogger("api.pipeline")
router = APIRouter(prefix="/v1/pipeline", tags=["pipeline"])

_AUTO_PROCESS_LIMIT = 5
_AUTO_CANDIDATE_LIMIT = 200


def _exc_detail(exc: HTTPException) -> str:
    detail = exc.detail
    if isinstance(detail, dict):
        return str(detail)
    return str(detail)


_LATEST_CLIENT_SQL = (
    text(
        """
        WITH ranked AS (
            SELECT
                inn,
                id,
                COALESCE(ended_at, started_at, created_at) AS sort_key,
                ROW_NUMBER() OVER (
                    PARTITION BY inn
                    ORDER BY COALESCE(ended_at, started_at, created_at) DESC NULLS LAST, id DESC
                ) AS rn
            FROM public.clients_requests
            WHERE inn IN :inns
        )
        SELECT inn, id
        FROM ranked
        WHERE rn = 1
        """
    ).bindparams(bindparam("inns", expanding=True))
)

_PARS_SITE_COUNT_SQL = (
    text(
        """
        SELECT company_id, COUNT(*) AS cnt
        FROM public.pars_site
        WHERE company_id IN :ids
        GROUP BY company_id
        """
    ).bindparams(bindparam("ids", expanding=True))
)

_AI_GOODS_COUNT_SQL = (
    text(
        """
        SELECT ps.company_id AS company_id, COUNT(*) AS cnt
        FROM public.ai_site_goods_types AS gt
        JOIN public.pars_site AS ps ON ps.id = gt.text_par_id
        WHERE ps.company_id IN :ids
        GROUP BY ps.company_id
        """
    ).bindparams(bindparam("ids", expanding=True))
)

_AI_EQUIPMENT_COUNT_SQL = (
    text(
        """
        SELECT ps.company_id AS company_id, COUNT(*) AS cnt
        FROM public.ai_site_equipment AS eq
        JOIN public.pars_site AS ps ON ps.id = eq.text_pars_id
        WHERE ps.company_id IN :ids
        GROUP BY ps.company_id
        """
    ).bindparams(bindparam("ids", expanding=True))
)

_EQUIPMENT_ALL_COUNT_SQL = (
    text(
        'SELECT company_id, COUNT(*) AS cnt FROM "EQUIPMENT_ALL" WHERE company_id IN :ids GROUP BY company_id'
    ).bindparams(bindparam("ids", expanding=True))
)


async def _fetch_latest_clients(
    conn: AsyncConnection, inns: Iterable[str]
) -> dict[str, int]:
    inn_list = []
    seen: set[str] = set()
    for inn in inns:
        inn_clean = str(inn or "").strip()
        if not inn_clean or inn_clean in seen:
            continue
        seen.add(inn_clean)
        inn_list.append(inn_clean)

    if not inn_list:
        return {}

    result = await conn.execute(_LATEST_CLIENT_SQL, {"inns": inn_list})
    mapping: dict[str, int] = {}
    for row in result.mappings():
        inn_val = str(row.get("inn") or "").strip()
        client_id = row.get("id")
        if not inn_val or client_id is None:
            continue
        try:
            mapping[inn_val] = int(client_id)
        except (TypeError, ValueError):
            continue
    return mapping


async def _fetch_counts(
    conn: AsyncConnection,
    stmt,
    ids: Iterable[int],
    *,
    ignore_errors: bool = False,
) -> dict[int, int]:
    id_list = [int(x) for x in ids if x is not None]
    if not id_list:
        return {}
    try:
        result = await conn.execute(stmt, {"ids": id_list})
    except SQLAlchemyError as exc:
        if ignore_errors:
            log.info("pipeline auto: пропуск чтения таблицы (%s)", exc)
            return {}
        raise
    counts: dict[int, int] = {}
    for row in result.mappings():
        company_id = row.get("company_id")
        if company_id is None:
            continue
        try:
            counts[int(company_id)] = int(row.get("cnt", 0) or 0)
        except (TypeError, ValueError):
            continue
    return counts


async def _collect_candidate_statuses(
    session: AsyncSession, engine: AsyncEngine
) -> list[PipelineAutoCandidate]:
    stmt = (
        select(DaDataResult.inn)
        .order_by(DaDataResult.updated_at.desc(), DaDataResult.inn.asc())
        .limit(_AUTO_CANDIDATE_LIMIT)
    )
    result = await session.execute(stmt)
    inns: list[str] = []
    seen: set[str] = set()
    for inn in result.scalars():
        inn_clean = str(inn or "").strip()
        if not inn_clean or inn_clean in seen:
            continue
        seen.add(inn_clean)
        inns.append(inn_clean)

    if not inns:
        return []

    async with engine.connect() as conn:
        clients_map = await _fetch_latest_clients(conn, inns)
        client_ids = list(clients_map.values())
        pars_counts = await _fetch_counts(conn, _PARS_SITE_COUNT_SQL, client_ids)
        goods_counts = await _fetch_counts(conn, _AI_GOODS_COUNT_SQL, client_ids)
        equip_counts = await _fetch_counts(conn, _AI_EQUIPMENT_COUNT_SQL, client_ids)
        equipment_selection_counts = await _fetch_counts(
            conn, _EQUIPMENT_ALL_COUNT_SQL, client_ids, ignore_errors=True
        )

    statuses: list[PipelineAutoCandidate] = []
    for inn in inns:
        client_id = clients_map.get(inn)
        has_client = client_id is not None

        def _has(data: dict[int, int]) -> bool:
            return bool(has_client and data.get(int(client_id)))

        statuses.append(
            PipelineAutoCandidate(
                inn=inn,
                client_request_id=client_id,
                has_clients_request=has_client,
                has_pars_site=_has(pars_counts),
                has_ai_goods=_has(goods_counts),
                has_ai_equipment=_has(equip_counts),
                has_equipment_selection=_has(equipment_selection_counts),
            )
        )
    return statuses


async def _run_step(
    name: str,
    runner: Callable[[], Awaitable[object]],
    *,
    errors: list[PipelineStepError],
    fatal: bool = False,
) -> tuple[object | None, bool]:
    try:
        result = await runner()
        return result, True
    except HTTPException as exc:
        log.warning("pipeline step %s failed with HTTPException: %s", name, exc.detail)
        errors.append(
            PipelineStepError(
                step=name,
                status_code=exc.status_code,
                detail=_exc_detail(exc),
            )
        )
        return None, not fatal
    except Exception as exc:  # noqa: BLE001
        log.exception("pipeline step %s crashed", name)
        errors.append(
            PipelineStepError(
                step=name,
                status_code=None,
                detail=str(exc),
            )
        )
        return None, not fatal


@router.post(
    "/full",
    response_model=PipelineFullResponse,
    summary="Полный пайплайн анализа клиента",
)
async def run_full_pipeline(
    payload: PipelineFullRequest,
    session: AsyncSession = Depends(get_bitrix_session),
) -> PipelineFullResponse:
    started = time.perf_counter()
    inn = payload.inn.strip()
    errors: list[PipelineStepError] = []

    lookup_result = None
    parse_result = None
    analyze_result = None
    ib_match_result = None
    equipment_result = None

    lookup_result, can_continue = await _run_step(
        "lookup_card",
        lambda: lookup_card_get(inn=inn, domain=None, session=session),
        errors=errors,
        fatal=True,
    )

    if can_continue:
        parse_result, _ = await _run_step(
            "parse_site",
            lambda: run_parse_site(ParseSiteRequest(inn=inn), session),
            errors=errors,
        )
    else:
        log.info("pipeline: lookup_card failed — skipping remaining steps for inn=%s", inn)

    if can_continue:
        analyze_refresh_site = bool(
            parse_result is not None and getattr(parse_result, "status", None) == "fallback"
        )
        analyze_result, _ = await _run_step(
            "analyze_json",
            lambda: analyze_from_inn_get(inn=inn, refresh_site=analyze_refresh_site),
            errors=errors,
        )

        ib_match_result, _ = await _run_step(
            "ib_match",
            lambda: ib_match_by_inn_get(inn=inn),
            errors=errors,
        )

        equipment_result, _ = await _run_step(
            "equipment_selection",
            lambda: get_equipment_selection_by_inn(inn=inn),
            errors=errors,
        )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return PipelineFullResponse(
        inn=inn,
        lookup_card=lookup_result,
        parse_site=parse_result,
        analyze_json=analyze_result,
        ib_match=ib_match_result,
        equipment_selection=equipment_result,
        errors=errors,
        duration_ms=duration_ms,
    )


@router.post(
    "/auto",
    response_model=PipelineAutoResponse,
    summary="Автозапуск пайплайна по незавершённым ИНН",
)
async def run_auto_pipeline(
    session: AsyncSession = Depends(get_bitrix_session),
) -> PipelineAutoResponse:
    engine = get_postgres_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="Postgres DSN is not configured")

    statuses = await _collect_candidate_statuses(session, engine)
    if not statuses:
        log.info("pipeline auto: кандидаты для обработки не найдены")
        return PipelineAutoResponse()

    pending = [status for status in statuses if not status.has_equipment_selection]
    to_process = pending[:_AUTO_PROCESS_LIMIT]

    processed: list[PipelineAutoResult] = []
    for status in to_process:
        try:
            payload = PipelineFullRequest(inn=status.inn)
            response = await run_full_pipeline(payload, session=session)
            processed.append(
                PipelineAutoResult(
                    status=status,
                    response=response,
                )
            )
        except HTTPException as exc:
            log.warning(
                "pipeline auto: HTTPException для inn=%s → %s", status.inn, exc.detail
            )
            processed.append(
                PipelineAutoResult(
                    status=status,
                    error=f"HTTP {exc.status_code}: {_exc_detail(exc)}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("pipeline auto: необработанная ошибка для inn=%s", status.inn)
            processed.append(
                PipelineAutoResult(
                    status=status,
                    error=str(exc),
                )
            )

    processed_inns = {item.status.inn for item in processed}
    skipped = [status for status in statuses if status.inn not in processed_inns]

    return PipelineAutoResponse(processed=processed, skipped=skipped)
