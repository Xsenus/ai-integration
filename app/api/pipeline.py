from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.db.postgres import get_postgres_engine
from app.schemas.pipeline import (
    ParseSiteStage,
    PipelineClient,
    PipelineParsSite,
    PipelineRequest,
    PipelineResponse,
    ResolvedIdentifiers,
)
from app.services.ai_analyzer import analyze_company_by_inn

log = logging.getLogger("api.pipeline")

router = APIRouter(prefix="/v1/pipeline", tags=["pipeline"])


@dataclass
class _ResolutionResult:
    client: dict[str, Any]
    pars: dict[str, Any] | None
    pars_list: list[dict[str, Any]]
    inn: str | None
    site: str | None


def _normalize_domain(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if "//" not in raw:
        raw = f"http://{raw}"
    try:
        parsed = urlparse(raw)
    except ValueError:
        return None
    host = (parsed.hostname or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


async def _fetch_client_by_id(conn: AsyncConnection, client_id: int) -> dict[str, Any] | None:
    q = text(
        """
        SELECT id, company_name, inn, domain_1, domain_2,
               started_at, ended_at, created_at
        FROM public.clients_requests
        WHERE id = :client_id
        LIMIT 1
        """
    )
    res = await conn.execute(q, {"client_id": client_id})
    row = res.mappings().first()
    return dict(row) if row else None


async def _fetch_clients_by_inn(conn: AsyncConnection, inn: str) -> list[dict[str, Any]]:
    q = text(
        """
        SELECT id, company_name, inn, domain_1, domain_2,
               started_at, ended_at, created_at
        FROM public.clients_requests
        WHERE inn = :inn
        ORDER BY COALESCE(ended_at, created_at) DESC NULLS LAST, id DESC
        """
    )
    res = await conn.execute(q, {"inn": inn})
    return [dict(row) for row in res.mappings().all()]


async def _fetch_clients_by_site(conn: AsyncConnection, domain: str) -> list[dict[str, Any]]:
    pattern = f"%{domain}%"
    q = text(
        """
        SELECT id, company_name, inn, domain_1, domain_2,
               started_at, ended_at, created_at
        FROM public.clients_requests
        WHERE COALESCE(domain_1, '') ILIKE :pattern
           OR COALESCE(domain_2, '') ILIKE :pattern
        ORDER BY COALESCE(ended_at, created_at) DESC NULLS LAST, id DESC
        """
    )
    res = await conn.execute(q, {"pattern": pattern})
    rows: list[dict[str, Any]] = []
    for row in res.mappings():
        data = dict(row)
        domains = [_normalize_domain(data.get("domain_1")), _normalize_domain(data.get("domain_2"))]
        if any(d == domain for d in domains if d):
            rows.append(data)
    return rows


async def _fetch_pars_by_id(conn: AsyncConnection, pars_id: int) -> dict[str, Any] | None:
    q = text(
        """
        SELECT id, company_id, domain_1, url, created_at
        FROM public.pars_site
        WHERE id = :pars_id
        LIMIT 1
        """
    )
    res = await conn.execute(q, {"pars_id": pars_id})
    row = res.mappings().first()
    return dict(row) if row else None


async def _fetch_pars_for_client(conn: AsyncConnection, client_id: int) -> list[dict[str, Any]]:
    q = text(
        """
        SELECT id, company_id, domain_1, url, created_at
        FROM public.pars_site
        WHERE company_id = :client_id
        ORDER BY created_at DESC NULLS LAST, id DESC
        """
    )
    res = await conn.execute(q, {"client_id": client_id})
    return [dict(row) for row in res.mappings().all()]


def _pick_pars_by_domain(rows: Iterable[dict[str, Any]], domain: str | None) -> dict[str, Any] | None:
    if not domain:
        return None
    for row in rows:
        row_domain = _normalize_domain(row.get("domain_1")) or _normalize_domain(row.get("url"))
        if row_domain == domain:
            return row
    return None


async def _resolve_identifiers(conn: AsyncConnection, payload: PipelineRequest) -> _ResolutionResult:
    inn = (payload.inn or "").strip() or None
    site_domain = _normalize_domain(payload.site) if payload.site else None

    pars_row = None
    client_row = None
    client_id = payload.client_id

    if payload.pars_id is not None:
        pars_row = await _fetch_pars_by_id(conn, payload.pars_id)
        if not pars_row:
            raise HTTPException(status_code=404, detail="pars_id не найден в pars_site")
        client_id = client_id or pars_row.get("company_id")

    if client_id is not None:
        client_row = await _fetch_client_by_id(conn, client_id)
        if not client_row:
            raise HTTPException(status_code=404, detail="client_id не найден в clients_requests")
        if pars_row and pars_row.get("company_id") != client_row["id"]:
            raise HTTPException(status_code=400, detail="Указанные client_id и pars_id принадлежат разным компаниям")

    if client_row is None and inn:
        candidates = await _fetch_clients_by_inn(conn, inn)
        if not candidates:
            raise HTTPException(status_code=404, detail="Не найдена запись clients_requests по ИНН")
        unique_ids = {c["id"] for c in candidates}
        if len(unique_ids) > 1 and payload.pars_id is None and payload.client_id is None:
            raise HTTPException(
                status_code=400,
                detail="Найдено несколько клиентов по ИНН. Уточните client_id или pars_id.",
            )
        client_row = candidates[0]
        client_id = client_row["id"]

    if client_row is None and site_domain:
        candidates = await _fetch_clients_by_site(conn, site_domain)
        if not candidates:
            raise HTTPException(status_code=404, detail="Не найдена запись clients_requests по сайту")
        unique_ids = {c["id"] for c in candidates}
        if len(unique_ids) > 1 and payload.pars_id is None:
            raise HTTPException(
                status_code=400,
                detail="Найдено несколько клиентов по сайту. Уточните client_id или pars_id.",
            )
        client_row = candidates[0]
        client_id = client_row["id"]

    if client_row is None:
        raise HTTPException(status_code=400, detail="Не удалось определить клиента по входным данным")

    resolved_inn = inn or (client_row.get("inn") or None)
    if inn and client_row.get("inn") and client_row["inn"] != inn:
        raise HTTPException(status_code=400, detail="Указанный client_id не принадлежит заданному ИНН")

    pars_list = await _fetch_pars_for_client(conn, client_row["id"])

    if payload.pars_id is not None and pars_row is None:
        pars_row = next((p for p in pars_list if p["id"] == payload.pars_id), None)
        if pars_row is None:
            raise HTTPException(status_code=404, detail="Указанный pars_id не относится к выбранному клиенту")

    if pars_row is None and site_domain:
        pars_row = _pick_pars_by_domain(pars_list, site_domain)

    if pars_row is None and pars_list:
        pars_row = pars_list[0]

    resolved_site = site_domain
    if not resolved_site:
        resolved_site = (
            _normalize_domain((pars_row or {}).get("domain_1"))
            or _normalize_domain((pars_row or {}).get("url"))
            or _normalize_domain(client_row.get("domain_1"))
            or _normalize_domain(client_row.get("domain_2"))
        )

    return _ResolutionResult(
        client=client_row,
        pars=pars_row,
        pars_list=pars_list,
        inn=resolved_inn,
        site=resolved_site,
    )


def _build_parse_stage(resolution: _ResolutionResult) -> ParseSiteStage:
    client = PipelineClient(**resolution.client)
    pars_sites = [PipelineParsSite(**{k: row.get(k) for k in ("id", "domain_1", "url", "created_at")}) for row in resolution.pars_list]
    selected_id = resolution.pars.get("id") if resolution.pars else None
    return ParseSiteStage(
        client_id=client.id,
        client=client,
        pars_sites=pars_sites,
        selected_pars_id=selected_id,
    )


def _skipped(detail: str) -> dict[str, Any]:
    return {"status": "skipped", "detail": detail}


@router.post("/full", response_model=PipelineResponse, summary="Полный пайплайн анализа клиента")
async def run_full_pipeline(payload: PipelineRequest) -> PipelineResponse:
    started = time.perf_counter()

    engine = get_postgres_engine()
    if engine is None:
        raise HTTPException(status_code=503, detail="POSTGRES_DATABASE_URL не задан — пайплайн недоступен")

    async with engine.connect() as conn:
        resolution = await _resolve_identifiers(conn, payload)

    parse_stage = _build_parse_stage(resolution)

    analyze_result: dict[str, Any] | None = None
    if payload.run_analyze:
        if resolution.inn:
            try:
                analyze_payload = await analyze_company_by_inn(resolution.inn)
                analyze_result = {"status": "ok", "payload": analyze_payload}
            except Exception as exc:  # noqa: BLE001
                log.exception("Pipeline analyze step failed for inn=%s", resolution.inn)
                analyze_result = {"status": "error", "detail": str(exc)}
        else:
            analyze_result = _skipped("Не удалось определить ИНН для шага анализа")

    response = PipelineResponse(
        resolved=ResolvedIdentifiers(
            inn=resolution.inn,
            site=resolution.site,
            pars_id=resolution.pars.get("id") if resolution.pars else None,
            client_id=resolution.client["id"],
        ),
        parse_site=parse_stage,
        analyze=analyze_result,
        ib_match=_skipped("Шаг IB-match не реализован в этой сборке"),
        equipment_selection=_skipped("Подбор оборудования не реализован в этой сборке"),
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return response
