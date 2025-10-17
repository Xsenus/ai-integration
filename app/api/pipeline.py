from __future__ import annotations

import logging
import time
from typing import Callable, Awaitable

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.lookup import lookup_card_get
from app.api.routes import (
    get_equipment_selection_by_inn,
    ib_match_by_inn_get,
    parse_site_by_inn,
)
from app.api.analyze_json import analyze_from_inn_get
from app.db.bitrix import get_bitrix_session
from app.schemas.pipeline import (
    PipelineFullRequest,
    PipelineFullResponse,
    PipelineStepError,
)

log = logging.getLogger("api.pipeline")
router = APIRouter(prefix="/v1/pipeline", tags=["pipeline"])


def _exc_detail(exc: HTTPException) -> str:
    detail = exc.detail
    if isinstance(detail, dict):
        return str(detail)
    return str(detail)


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
            lambda: parse_site_by_inn(inn=inn, session=session),
            errors=errors,
        )
    else:
        log.info("pipeline: lookup_card failed — skipping remaining steps for inn=%s", inn)

    if can_continue:
        analyze_result, _ = await _run_step(
            "analyze_json",
            lambda: analyze_from_inn_get(inn=inn),
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
