from __future__ import annotations

import asyncio
import logging
import copy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional

import httpx
from fastapi import APIRouter, HTTPException, status
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.exc import SQLAlchemyError

from app.api.routes import ParseSiteRequest, _parse_site_impl
from app.config import settings
from app.db.bitrix import bitrix_session
from app.db.parsing_mirror import _get_pars_site_columns
from app.db.postgres import get_postgres_engine
from app.schemas.analyze_json import (
    AnalyzeFromInnError,
    AnalyzeFromInnRequest,
    AnalyzeFromInnResponse,
    AnalyzeFromInnRun,
)

log = logging.getLogger("api.analyze-json")
router = APIRouter(prefix="/v1", tags=["analyze-json"])

_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock()

_TEXT_PREVIEW_LIMIT = 400


def _summarize_external_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Готовит компактное представление тела запроса для логов."""

    summary: dict[str, Any] = {}
    for key, value in payload.items():
        if key == "text_par" and isinstance(value, str):
            summary[key] = {
                "length": len(value),
                "preview": value[:_TEXT_PREVIEW_LIMIT],
            }
        elif key in {"goods_catalog", "equipment_catalog"} and isinstance(value, list):
            summary[key] = {"items": len(value)}
        else:
            summary[key] = value
    return summary


def _log_and_raise(
    status_code: int,
    *,
    inn: str,
    detail: str,
    request_context: Mapping[str, Any],
    extra: Optional[Mapping[str, Any]] = None,
    level: int = logging.ERROR,
) -> None:
    """Формирует структурированную ошибку, логирует и выбрасывает HTTPException."""

    payload: dict[str, Any] = {"request": dict(request_context)} if request_context else {}
    if extra:
        payload["extra"] = dict(extra)

    log.log(
        level,
        "analyze-json: %s (inn=%s, status=%s, context=%s)",
        detail,
        inn,
        status_code,
        payload,
    )

    error = AnalyzeFromInnError(
        inn=inn,
        detail=detail,
        payload=payload or None,
    )
    raise HTTPException(status_code=status_code, detail=error.model_dump())


@dataclass(slots=True)
class ParsSiteSnapshot:
    pars_id: int
    company_id: int
    text: str
    created_at: datetime
    chunk_ids: list[int]
    domain: Optional[str]
    url: Optional[str]
    domain_source: Optional[str] = None


async def _get_http_client() -> httpx.AsyncClient:
    """Лениво создаёт httpx-клиент с таймаутом из настроек."""

    global _http_client
    if _http_client is not None:
        return _http_client

    async with _http_client_lock:
        if _http_client is None:
            timeout = settings.analyze_timeout
            log.info(
                "analyze-json: creating HTTP client for external service (timeout=%ss)",
                timeout,
            )
            _http_client = httpx.AsyncClient(timeout=timeout)
    return _http_client


async def close_analyze_json_http_client() -> None:
    """Закрывает HTTP-клиент внешнего сервиса (используется при shutdown)."""

    global _http_client
    if _http_client is not None:
        try:
            await _http_client.aclose()
        finally:
            _http_client = None


async def _trigger_parse_site(inn: str) -> None:
    """Запускает /v1/parse-site для указанного ИНН (best effort)."""

    payload = ParseSiteRequest(inn=inn, save_client_request=True)
    try:
        log.info("analyze-json: triggering parse-site (inn=%s)", inn)
        async with bitrix_session() as session:
            await _parse_site_impl(payload, session)
        log.info("analyze-json: pars_site refreshed for inn=%s", inn)
    except HTTPException as exc:
        log.info("analyze-json: parse-site skipped for inn=%s → %s", inn, exc.detail)
    except Exception:  # noqa: BLE001
        log.exception("analyze-json: parse-site failed for inn=%s", inn)


def _ensure_identifier(name: str) -> str:
    """Проверяет SQL-идентификатор (таблица/колонка) и возвращает его в виде public."""

    if not name:
        raise ValueError("Empty identifier")
    if not all(ch.isalnum() or ch == "_" for ch in name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def _normalize_catalog_vector(value: Any) -> Any:
    """Приводит значение вектора каталога к str | list | None."""

    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        result: list[float] = []
        for part in value:
            try:
                result.append(float(part))
            except Exception:  # noqa: BLE001
                continue
        return result or None
    if isinstance(value, memoryview):
        try:
            value = value.tobytes()
        except Exception:  # noqa: BLE001
            return None
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:  # noqa: BLE001
            return value.hex()
    return value


async def _load_catalog(
    engine: AsyncEngine,
    *,
    table: str,
    id_col: str,
    name_col: str,
    vec_col: str,
) -> list[dict[str, Any]]:
    """Читает каталог из таблицы с указанными колонками."""

    table_name = _ensure_identifier(table)
    id_column = _ensure_identifier(id_col)
    name_column = _ensure_identifier(name_col)
    vec_column = _ensure_identifier(vec_col)

    sql = text(
        f"""
        SELECT {id_column} AS id,
               {name_column} AS name,
               {vec_column} AS vec
        FROM public.{table_name}
        ORDER BY {id_column}
        """
    )

    log.info(
        "analyze-json: loading catalog from public.%s (id_col=%s, name_col=%s, vec_col=%s) using SQL: %s",
        table_name,
        id_column,
        name_column,
        vec_column,
        sql.text.strip(),
    )

    async with engine.connect() as conn:
        res: Result = await conn.execute(sql)
        rows = res.mappings().all()

    catalog: list[dict[str, Any]] = []
    for row in rows:
        item_id = row.get("id")
        name = (row.get("name") or "").strip()
        if not name:
            continue
        vec = _normalize_catalog_vector(row.get("vec"))
        catalog.append({"id": item_id, "name": name, "vec": vec})
    log.info(
        "analyze-json: catalog loaded from public.%s → %s items",
        table_name,
        len(catalog),
    )
    return catalog


def _build_snapshot_from_rows(
    rows: list[Mapping[str, Any]],
    *,
    company_id: int,
    created_at: datetime,
    domain_hint: Optional[str],
    domain_source: Optional[str],
) -> Optional[ParsSiteSnapshot]:
    """Формирует ParsSiteSnapshot из строк pars_site."""

    if not rows:
        return None

    chunk_ids: list[int] = []
    text_parts: list[str] = []
    domain_value: Optional[str] = domain_hint.strip() if isinstance(domain_hint, str) else domain_hint
    url_value: Optional[str] = None

    for row in rows:
        chunk_id = row.get("id")
        if chunk_id is not None:
            try:
                chunk_ids.append(int(chunk_id))
            except Exception:  # noqa: BLE001
                continue

        if not domain_value:
            raw_domain_1 = row.get("domain_1")
            if raw_domain_1:
                domain_value = str(raw_domain_1).strip() or domain_value
        raw_domain_2 = row.get("domain_2") if isinstance(row, Mapping) else None
        if raw_domain_2:
            raw_domain_2_str = str(raw_domain_2).strip()
            if raw_domain_2_str:
                domain_value = raw_domain_2_str

        if not url_value:
            raw_url = row.get("url")
            if raw_url:
                url_value = str(raw_url)

        value = row.get("text_par") or row.get("text")
        if value is None:
            continue
        text = str(value).strip()
        if text:
            text_parts.append(text)

    if not text_parts or not chunk_ids:
        return None

    full_text = "\n\n".join(text_parts).strip()
    if not full_text:
        return None

    pars_id = min(chunk_ids)
    return ParsSiteSnapshot(
        pars_id=pars_id,
        company_id=company_id,
        text=full_text,
        created_at=created_at,
        chunk_ids=chunk_ids,
        domain=domain_value,
        url=url_value,
        domain_source=domain_source,
    )


async def _collect_snapshot_for_domain(
    conn: AsyncConnection,
    columns: set[str],
    select_clause: str,
    *,
    company_id: int,
    domain_value: str,
    domain_field: str,
    inn: str,
) -> Optional[ParsSiteSnapshot]:
    """Читает последний набор чанков pars_site для конкретного домена."""

    domain_value = domain_value.strip()
    if not domain_value:
        return None

    domain_clauses: list[str] = []
    if "domain_1" in columns:
        domain_clauses.append("LOWER(ps.domain_1) = LOWER(:domain)")
    if "domain_2" in columns:
        domain_clauses.append("LOWER(ps.domain_2) = LOWER(:domain)")

    if not domain_clauses:
        log.info(
            "analyze-json: skipping domain snapshot due to missing domain columns (inn=%s, company_id=%s, domain=%s)",
            inn,
            company_id,
            domain_value,
        )
        return None

    domain_condition = " OR ".join(domain_clauses)
    if len(domain_clauses) > 1:
        domain_condition = f"({domain_condition})"

    sql_created_at = text(
        f"""
        SELECT MAX(ps.created_at) AS created_at
        FROM public.pars_site AS ps
        WHERE ps.company_id = :company_id
          AND {domain_condition}
        """
    )
    log.info(
        "analyze-json: querying public.pars_site for latest created_at (inn=%s, company_id=%s, domain_field=%s, domain=%s) using SQL: %s",
        inn,
        company_id,
        domain_field,
        domain_value,
        sql_created_at.text.strip(),
    )
    created_row = (
        await conn.execute(
            sql_created_at,
            {"company_id": company_id, "domain": domain_value},
        )
    ).mappings().first()

    if not created_row or not created_row.get("created_at"):
        log.info(
            "analyze-json: no pars_site.created_at found (inn=%s, company_id=%s, domain=%s)",
            inn,
            company_id,
            domain_value,
        )
        return None

    created_at = created_row["created_at"]

    sql_chunks = text(
        f"""
        SELECT {select_clause}
        FROM public.pars_site AS ps
        WHERE ps.company_id = :company_id
          AND ps.created_at = :created_at
          AND {domain_condition}
        ORDER BY ps.id
        """
    )
    log.info(
        "analyze-json: loading pars_site chunks (inn=%s, company_id=%s, domain_field=%s, domain=%s, created_at=%s) using SQL: %s",
        inn,
        company_id,
        domain_field,
        domain_value,
        created_at,
        sql_chunks.text.strip(),
    )
    rows = (
        await conn.execute(
            sql_chunks,
            {
                "company_id": company_id,
                "created_at": created_at,
                "domain": domain_value,
            },
        )
    ).mappings().all()

    if not rows:
        log.info(
            "analyze-json: pars_site rows not found (inn=%s, company_id=%s, domain=%s, created_at=%s)",
            inn,
            company_id,
            domain_value,
            created_at,
        )
        return None

    snapshot = _build_snapshot_from_rows(
        rows,
        company_id=company_id,
        created_at=created_at,
        domain_hint=domain_value,
        domain_source=domain_field,
    )
    if snapshot:
        log.info(
            "analyze-json: snapshot built (inn=%s, company_id=%s, domain=%s, pars_id=%s, text_len=%s, chunks=%s)",
            inn,
            company_id,
            snapshot.domain,
            snapshot.pars_id,
            len(snapshot.text),
            len(snapshot.chunk_ids),
        )
    else:
        log.info(
            "analyze-json: snapshot empty after parsing rows (inn=%s, company_id=%s, domain=%s)",
            inn,
            company_id,
            domain_value,
        )
    return snapshot


async def _collect_latest_snapshot_any(
    conn: AsyncConnection,
    columns: set[str],
    select_clause: str,
    inn: str,
) -> Optional[ParsSiteSnapshot]:
    """Фолбэк: читает последний набор чанков без учёта домена."""

    sql_latest = text(
        """
        SELECT ps.created_at, ps.company_id
        FROM public.pars_site AS ps
        JOIN public.clients_requests AS cr ON cr.id = ps.company_id
        WHERE cr.inn = :inn
        ORDER BY ps.created_at DESC NULLS LAST, ps.id DESC
        LIMIT 1
        """
    )
    log.info(
        "analyze-json: fallback query for latest pars_site (inn=%s) using SQL: %s",
        inn,
        sql_latest.text.strip(),
    )
    latest = (await conn.execute(sql_latest, {"inn": inn})).mappings().first()
    if not latest:
        log.info("analyze-json: fallback latest pars_site not found (inn=%s)", inn)
        return None

    created_at = latest.get("created_at")
    company_id = latest.get("company_id")
    if created_at is None or company_id is None:
        log.info(
            "analyze-json: fallback latest pars_site has empty created_at/company_id (inn=%s)",
            inn,
        )
        return None

    sql_chunks = text(
        f"""
        SELECT {select_clause}
        FROM public.pars_site AS ps
        WHERE ps.company_id = :company_id
          AND ps.created_at = :created_at
        ORDER BY ps.id
        """
    )
    log.info(
        "analyze-json: fallback loading pars_site chunks (inn=%s, company_id=%s, created_at=%s) using SQL: %s",
        inn,
        company_id,
        created_at,
        sql_chunks.text.strip(),
    )
    rows = (
        await conn.execute(
            sql_chunks,
            {"company_id": company_id, "created_at": created_at},
        )
    ).mappings().all()

    snapshot = _build_snapshot_from_rows(
        rows,
        company_id=int(company_id),
        created_at=created_at,
        domain_hint=None,
        domain_source=None,
    )
    if snapshot:
        log.info(
            "analyze-json: fallback snapshot built (inn=%s, company_id=%s, pars_id=%s, text_len=%s, chunks=%s)",
            inn,
            company_id,
            snapshot.pars_id,
            len(snapshot.text),
            len(snapshot.chunk_ids),
        )
    else:
        log.info(
            "analyze-json: fallback snapshot empty after parsing rows (inn=%s, company_id=%s)",
            inn,
            company_id,
        )
    return snapshot


async def _collect_latest_pars_site(engine: AsyncEngine, inn: str) -> list[ParsSiteSnapshot]:
    """Возвращает список последних наборов чанков pars_site для доменов компании."""

    log.info("analyze-json: collecting pars_site snapshots (inn=%s)", inn)
    columns = await _get_pars_site_columns()
    log.info("analyze-json: detected pars_site columns for inn=%s → %s", inn, sorted(columns))

    select_fields: list[str] = ["ps.id"]

    text_columns = []
    if "text_par" in columns:
        select_fields.append("ps.text_par")
        text_columns.append("text_par")
    if "text" in columns:
        select_fields.append("ps.text")
        text_columns.append("text")

    if not text_columns:
        log.error(
            "analyze-json: pars_site table is missing text columns (expected one of text_par/text)",
        )
        return []

    if "domain_1" in columns:
        select_fields.append("ps.domain_1")
    if "url" in columns:
        select_fields.append("ps.url")
    if "created_at" in columns:
        select_fields.append("ps.created_at")
    else:
        log.error("analyze-json: pars_site table is missing created_at column")
        return []
    if "domain_2" in columns:
        select_fields.append("ps.domain_2")
    select_clause = ", ".join(select_fields)

    sql_clients = text(
        """
        SELECT id, domain_1, domain_2
        FROM public.clients_requests
        WHERE inn = :inn
        ORDER BY id DESC
        """
    )
    log.info(
        "analyze-json: querying clients_requests for domains (inn=%s) using SQL: %s",
        inn,
        sql_clients.text.strip(),
    )

    snapshots: list[ParsSiteSnapshot] = []
    processed_domains: set[tuple[str, str]] = set()

    async with engine.connect() as conn:
        client_rows = (await conn.execute(sql_clients, {"inn": inn})).mappings().all()
        log.info(
            "analyze-json: clients_requests rows loaded for inn=%s → %s",
            inn,
            len(client_rows),
        )

        for row in client_rows:
            company_id = row.get("id")
            if company_id is None:
                continue
            for domain_field in ("domain_1", "domain_2"):
                if domain_field not in columns:
                    continue
                raw_domain = row.get(domain_field)
                domain_value = str(raw_domain).strip() if raw_domain else ""
                if not domain_value:
                    continue
                key = (domain_field, domain_value.lower())
                if key in processed_domains:
                    log.info(
                        "analyze-json: domain skipped as duplicate (inn=%s, company_id=%s, domain_field=%s, domain=%s)",
                        inn,
                        company_id,
                        domain_field,
                        domain_value,
                    )
                    continue
                log.info(
                    "analyze-json: attempt to build snapshot (inn=%s, company_id=%s, domain_field=%s, domain=%s)",
                    inn,
                    company_id,
                    domain_field,
                    domain_value,
                )
                snapshot = await _collect_snapshot_for_domain(
                    conn,
                    columns,
                    select_clause,
                    company_id=int(company_id),
                    domain_value=domain_value,
                    domain_field=domain_field,
                    inn=inn,
                )
                processed_domains.add(key)
                if snapshot:
                    snapshots.append(snapshot)

        if not snapshots:
            log.info(
                "analyze-json: no domain-specific pars_site snapshots found for inn=%s, running fallback",
                inn,
            )
            fallback_snapshot = await _collect_latest_snapshot_any(conn, columns, select_clause, inn)
            if fallback_snapshot:
                snapshots.append(fallback_snapshot)

    log.info(
        "analyze-json: total snapshots collected for inn=%s → %s",
        inn,
        len(snapshots),
    )
    return snapshots


def _vector_to_literal(data: Any) -> Optional[str]:
    """Конвертирует структуру VectorPayload в строковое представление для pgvector."""

    if data is None:
        return None
    if isinstance(data, str):
        literal = data.strip()
        return literal or None
    if isinstance(data, dict):
        literal = data.get("literal")
        if isinstance(literal, str) and literal.strip():
            return literal.strip()
        values = data.get("values")
        if isinstance(values, Iterable):
            try:
                values_list = [float(v) for v in values]
            except Exception:  # noqa: BLE001
                return None
            return "[" + ",".join(f"{v:.6f}" for v in values_list) + "]"
        return None
    if isinstance(data, Iterable):
        try:
            values_list = [float(v) for v in data]
        except Exception:  # noqa: BLE001
            return None
        return "[" + ",".join(f"{v:.6f}" for v in values_list) + "]"
    return None


def _normalize_score(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        num = float(value)
    except Exception:  # noqa: BLE001
        return None
    if num > 1.0 and num <= 100.0:
        num = num / 100.0
    num = max(0.0, min(1.0, num))
    return float(f"{num:.2f}")


async def _prodclass_exists(conn: AsyncConnection, prodclass_id: int) -> bool:
    """Проверяет наличие prodclass в справочнике ib_prodclass."""

    check_sql = text(
        "SELECT 1 FROM public.ib_prodclass WHERE id = :prodclass_id LIMIT 1"
    )
    try:
        result = await conn.execute(check_sql, {"prodclass_id": prodclass_id})
    except SQLAlchemyError:
        log.warning(
            "analyze-json: failed to verify prodclass in ib_prodclass (prodclass=%s)",
            prodclass_id,
            exc_info=True,
        )
        return False

    return result.scalar_one_or_none() is not None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return None


async def _apply_db_payload(
    engine: AsyncEngine,
    snapshot: ParsSiteSnapshot,
    payload: dict[str, Any],
) -> tuple[int, int, Optional[int], Optional[float]]:
    """Сохраняет db_payload из ответа внешнего сервиса."""

    goods_saved = 0
    equipment_saved = 0
    prodclass_id: Optional[int] = None
    prodclass_score: Optional[float] = None

    log.info(
        "analyze-json: applying db_payload (pars_id=%s, company_id=%s)",
        snapshot.pars_id,
        snapshot.company_id,
    )

    cols = await _get_pars_site_columns()

    description_present = "description" in payload
    description_value = str(payload.get("description") or "").strip() if description_present else None

    vector_present = "description_vector" in payload
    vector_literal = _vector_to_literal(payload.get("description_vector")) if vector_present else None

    async with engine.begin() as conn:
        if description_present and "description" in cols:
            update_sql = "UPDATE public.pars_site SET description = :description WHERE id = :id"
            log.info(
                "analyze-json: updating description in public.pars_site (pars_id=%s, length=%s) using SQL: %s",
                snapshot.pars_id,
                len(description_value or ""),
                update_sql,
            )
            await conn.execute(
                text(update_sql),
                {"description": description_value or None, "id": snapshot.pars_id},
            )

        if vector_present and "text_vector" in cols:
            update_sql = (
                "UPDATE public.pars_site "
                "SET text_vector = CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END "
                "WHERE id = :id"
            )
            log.info(
                "analyze-json: updating text_vector in public.pars_site (pars_id=%s, vector_present=%s) using SQL: %s",
                snapshot.pars_id,
                bool(vector_literal),
                update_sql,
            )
            await conn.execute(text(update_sql), {"vec": vector_literal, "id": snapshot.pars_id})

        if "prodclass" in payload:
            delete_prodclass_sql = "DELETE FROM public.ai_site_prodclass WHERE text_pars_id = :pid"
            log.info(
                "analyze-json: clearing public.ai_site_prodclass for pars_id=%s using SQL: %s",
                snapshot.pars_id,
                delete_prodclass_sql,
            )
            await conn.execute(text(delete_prodclass_sql), {"pid": snapshot.pars_id})
            raw_prod = payload.get("prodclass")
            if isinstance(raw_prod, dict):
                candidate_prodclass_id = _safe_int(
                    raw_prod.get("id")
                    or raw_prod.get("prodclass")
                    or raw_prod.get("prodclass_id")
                )
                candidate_prodclass_score = _normalize_score(
                    raw_prod.get("score") or raw_prod.get("prodclass_score")
                )
                if candidate_prodclass_id is not None:
                    if await _prodclass_exists(conn, candidate_prodclass_id):
                        insert_prodclass_sql = (
                            "INSERT INTO public.ai_site_prodclass "
                            "(text_pars_id, prodclass, prodclass_score) "
                            "VALUES (:pid, :prodclass, :score)"
                        )
                        log.info(
                            "analyze-json: inserting prodclass (pars_id=%s, prodclass=%s, score=%s) using SQL: %s",
                            snapshot.pars_id,
                            candidate_prodclass_id,
                            candidate_prodclass_score,
                            insert_prodclass_sql,
                        )
                        await conn.execute(
                            text(insert_prodclass_sql),
                            {
                                "pid": snapshot.pars_id,
                                "prodclass": candidate_prodclass_id,
                                "score": candidate_prodclass_score,
                            },
                        )
                        prodclass_id = candidate_prodclass_id
                        prodclass_score = candidate_prodclass_score
                    else:
                        log.warning(
                            "analyze-json: skipping prodclass insert due to missing ib_prodclass entry (pars_id=%s, prodclass=%s)",
                            snapshot.pars_id,
                            candidate_prodclass_id,
                        )

        if "goods_types" in payload:
            delete_goods_sql = "DELETE FROM public.ai_site_goods_types WHERE text_par_id = :pid"
            log.info(
                "analyze-json: clearing public.ai_site_goods_types for pars_id=%s using SQL: %s",
                snapshot.pars_id,
                delete_goods_sql,
            )
            await conn.execute(text(delete_goods_sql), {"pid": snapshot.pars_id})
            goods_items = payload.get("goods_types")
            if isinstance(goods_items, list):
                insert_sql = text(
                    """
                    INSERT INTO public.ai_site_goods_types
                        (text_par_id, goods_type, goods_types_score, goods_type_ID, text_vector)
                    VALUES
                        (:pid, :name, :score, :match_id,
                         CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END)
                    """
                )
                for item in goods_items:
                    if not isinstance(item, dict):
                        continue
                    name = (item.get("name") or item.get("goods_type") or "").strip()
                    if not name:
                        continue
                    match_id = _safe_int(item.get("match_id") or item.get("id") or item.get("goods_type_ID"))
                    score = _normalize_score(item.get("match_score") or item.get("score"))
                    vec_literal = _vector_to_literal(item.get("vector") or item.get("text_vector"))
                    log.info(
                        "analyze-json: inserting goods_type (pars_id=%s, name=%s, match_id=%s, score=%s)",
                        snapshot.pars_id,
                        name,
                        match_id,
                        score,
                    )
                    await conn.execute(
                        insert_sql,
                        {
                            "pid": snapshot.pars_id,
                            "name": name,
                            "score": score,
                            "match_id": match_id,
                            "vec": vec_literal,
                        },
                    )
                    goods_saved += 1

        if "equipment" in payload:
            delete_equipment_sql = "DELETE FROM public.ai_site_equipment WHERE text_pars_id = :pid"
            log.info(
                "analyze-json: clearing public.ai_site_equipment for pars_id=%s using SQL: %s",
                snapshot.pars_id,
                delete_equipment_sql,
            )
            await conn.execute(text(delete_equipment_sql), {"pid": snapshot.pars_id})
            equipment_items = payload.get("equipment")
            if isinstance(equipment_items, list):
                insert_sql = text(
                    """
                    INSERT INTO public.ai_site_equipment
                        (text_pars_id, equipment, equipment_score, equipment_ID, text_vector)
                    VALUES
                        (:pid, :name, :score, :match_id,
                         CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END)
                    """
                )
                for item in equipment_items:
                    if not isinstance(item, dict):
                        continue
                    name = (item.get("name") or item.get("equipment") or "").strip()
                    if not name:
                        continue
                    match_id = _safe_int(item.get("match_id") or item.get("id") or item.get("equipment_ID"))
                    score = _normalize_score(item.get("match_score") or item.get("score"))
                    vec_literal = _vector_to_literal(item.get("vector") or item.get("text_vector"))
                    log.info(
                        "analyze-json: inserting equipment (pars_id=%s, name=%s, match_id=%s, score=%s)",
                        snapshot.pars_id,
                        name,
                        match_id,
                        score,
                    )
                    await conn.execute(
                        insert_sql,
                        {
                            "pid": snapshot.pars_id,
                            "name": name,
                            "score": score,
                            "match_id": match_id,
                            "vec": vec_literal,
                        },
                    )
                    equipment_saved += 1

    log.info(
        "analyze-json: db_payload applied (pars_id=%s, goods_saved=%s, equipment_saved=%s, prodclass=%s, prodclass_score=%s)",
        snapshot.pars_id,
        goods_saved,
        equipment_saved,
        prodclass_id,
        prodclass_score,
    )
    return goods_saved, equipment_saved, prodclass_id, prodclass_score


async def _run_analyze(
    payload: AnalyzeFromInnRequest,
    *,
    source: str,
) -> AnalyzeFromInnResponse:
    """Общий пайплайн анализа с подробным логированием."""

    payload_data = payload.model_dump()
    inn = (payload_data.get("inn") or "").strip()
    payload_data["inn"] = inn
    payload_data["source"] = source

    log.info("analyze-json: %s request accepted → %s", source, payload_data)

    if not inn:
        _log_and_raise(
            status.HTTP_400_BAD_REQUEST,
            inn=inn,
            detail="ИНН не может быть пустым",
            request_context=payload_data,
            level=logging.WARNING,
        )

    engine = get_postgres_engine()
    if engine is None:
        _log_and_raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            inn=inn,
            detail="Postgres DSN is not configured",
            request_context=payload_data,
        )

    base_url = settings.analyze_base
    if not base_url:
        _log_and_raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            inn=inn,
            detail="ANALYZE_BASE is not configured",
            request_context=payload_data,
        )

    if payload.refresh_site:
        await _trigger_parse_site(inn)

    snapshots = await _collect_latest_pars_site(engine, inn)
    if not snapshots and not payload.refresh_site:
        await _trigger_parse_site(inn)
        snapshots = await _collect_latest_pars_site(engine, inn)

    if not snapshots:
        _log_and_raise(
            status.HTTP_404_NOT_FOUND,
            inn=inn,
            detail="В pars_site нет текста для указанного ИНН",
            request_context=payload_data,
            level=logging.WARNING,
        )

    log.info(
        "analyze-json: snapshots to process for inn=%s → %s",
        inn,
        len(snapshots),
    )
    log.info(
        "analyze-json: domains detected for inn=%s → %s",
        inn,
        [snap.domain or "(unknown)" for snap in snapshots],
    )

    goods_catalog: list[dict[str, Any]] = []
    equipment_catalog: list[dict[str, Any]] = []
    if payload.include_catalogs:
        try:
            goods_catalog = await _load_catalog(
                engine,
                table=settings.IB_GOODS_TYPES_TABLE,
                id_col=settings.IB_GOODS_TYPES_ID_COLUMN,
                name_col=settings.IB_GOODS_TYPES_NAME_COLUMN,
                vec_col=settings.IB_GOODS_TYPES_VECTOR_COLUMN,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("analyze-json: failed to load goods catalog: %s", exc)
            goods_catalog = []
        try:
            equipment_catalog = await _load_catalog(
                engine,
                table=settings.IB_EQUIPMENT_TABLE,
                id_col=settings.IB_EQUIPMENT_ID_COLUMN,
                name_col=settings.IB_EQUIPMENT_NAME_COLUMN,
                vec_col=settings.IB_EQUIPMENT_VECTOR_COLUMN,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("analyze-json: failed to load equipment catalog: %s", exc)
            equipment_catalog = []
    else:
        log.info("analyze-json: catalogs loading skipped by request")

    base_endpoint = base_url.rstrip("/")
    log.info("analyze-json: external endpoint resolved → %s", base_endpoint)
    client = await _get_http_client()

    runs: list[AnalyzeFromInnRun] = []
    total_text_length = 0
    total_saved_goods = 0
    total_saved_equipment = 0
    domains_processed: list[str] = []

    for idx, snapshot in enumerate(snapshots, start=1):
        domain_label = snapshot.domain or ""
        domain_source = snapshot.domain_source or ("domain_1" if domain_label else "auto")
        domains_processed.append(domain_label or "(unknown)")
        text_length = len(snapshot.text)
        snapshot_context = {
            "pars_id": snapshot.pars_id,
            "company_id": snapshot.company_id,
            "domain": domain_label or None,
            "domain_source": domain_source,
            "chunks": len(snapshot.chunk_ids),
            "text_length": text_length,
        }
        log.info(
            "analyze-json: preparing request #%s (inn=%s, snapshot=%s)",
            idx,
            inn,
            snapshot_context,
        )

        request_payload: dict[str, Any] = {
            "pars_id": snapshot.pars_id,
            "company_id": snapshot.company_id,
            "text_par": snapshot.text,
        }
        request_payload_for_response = copy.deepcopy(request_payload)
        if payload.chat_model:
            request_payload["chat_model"] = payload.chat_model
            request_payload_for_response["chat_model"] = payload.chat_model
        elif settings.CHAT_MODEL:
            request_payload["chat_model"] = settings.CHAT_MODEL
            request_payload_for_response["chat_model"] = settings.CHAT_MODEL
        if payload.embed_model:
            request_payload["embed_model"] = payload.embed_model
            request_payload_for_response["embed_model"] = payload.embed_model
        elif settings.embed_model:
            request_payload["embed_model"] = settings.embed_model
            request_payload_for_response["embed_model"] = settings.embed_model
        if payload.return_prompt is not None:
            request_payload["return_prompt"] = payload.return_prompt
            request_payload_for_response["return_prompt"] = payload.return_prompt
        if payload.return_answer_raw is not None:
            request_payload["return_answer_raw"] = payload.return_answer_raw
            request_payload_for_response["return_answer_raw"] = payload.return_answer_raw
        if goods_catalog:
            request_payload["goods_catalog"] = goods_catalog
            request_payload_for_response["goods_catalog"] = copy.deepcopy(goods_catalog)
        if equipment_catalog:
            request_payload["equipment_catalog"] = equipment_catalog
            request_payload_for_response["equipment_catalog"] = copy.deepcopy(
                equipment_catalog
            )

        payload_summary = _summarize_external_payload(request_payload)

        run_url = f"{base_endpoint}/v1/analyze/{snapshot.pars_id}"
        log.info(
            "analyze-json: sending request #%s to external service (inn=%s, url=%s, payload=%s)",
            idx,
            inn,
            run_url,
            payload_summary,
        )

        try:
            response = await client.post(run_url, json=request_payload)
        except httpx.RequestError as exc:
            detail = f"Не удалось выполнить запрос к внешнему сервису: {exc}".strip()
            _log_and_raise(
                status.HTTP_502_BAD_GATEWAY,
                inn=inn,
                detail=detail,
                request_context=payload_data,
                extra={
                    "url": run_url,
                    "snapshot": snapshot_context,
                    "payload": payload_summary,
                },
            )

        log.info(
            "analyze-json: response received #%s (inn=%s, domain=%s, status=%s)",
            idx,
            inn,
            domain_label,
            response.status_code,
        )

        if response.status_code >= 400:
            detail = f"Внешний сервис вернул ошибку: HTTP {response.status_code}"
            _log_and_raise(
                status.HTTP_502_BAD_GATEWAY,
                inn=inn,
                detail=detail,
                request_context=payload_data,
                extra={
                    "url": run_url,
                    "snapshot": snapshot_context,
                    "payload": payload_summary,
                    "status": response.status_code,
                    "body": response.text or "",
                },
            )

        try:
            response_json = response.json()
        except Exception as exc:  # noqa: BLE001
            detail = f"Не удалось распарсить ответ внешнего сервиса: {exc}".strip()
            _log_and_raise(
                status.HTTP_502_BAD_GATEWAY,
                inn=inn,
                detail=detail,
                request_context=payload_data,
                extra={
                    "url": run_url,
                    "snapshot": snapshot_context,
                    "payload": payload_summary,
                    "status": response.status_code,
                    "body": response.text,
                },
            )

        db_payload = response_json.get("db_payload") if isinstance(response_json, dict) else None
        if not isinstance(db_payload, dict):
            _log_and_raise(
                status.HTTP_502_BAD_GATEWAY,
                inn=inn,
                detail="Во внешнем ответе отсутствует блок db_payload",
                request_context=payload_data,
                extra={
                    "url": run_url,
                    "snapshot": snapshot_context,
                    "payload": payload_summary,
                    "status": response.status_code,
                    "response": response_json,
                },
            )

        goods_saved, equipment_saved, prodclass_id, prodclass_score = await _apply_db_payload(
            engine,
            snapshot,
            db_payload,
        )

        log.info(
            "analyze-json: run #%s stored (inn=%s, domain=%s, goods_saved=%s, equipment_saved=%s, prodclass=%s)",
            idx,
            inn,
            domain_label,
            goods_saved,
            equipment_saved,
            prodclass_id,
        )

        total_text_length += text_length
        total_saved_goods += goods_saved
        total_saved_equipment += equipment_saved

        run = AnalyzeFromInnRun(
            domain=domain_label or None,
            domain_source=domain_source,
            pars_id=snapshot.pars_id,
            company_id=snapshot.company_id,
            created_at=snapshot.created_at,
            text_length=text_length,
            chunk_count=len(snapshot.chunk_ids),
            catalog_goods_size=len(goods_catalog),
            catalog_equipment_size=len(equipment_catalog),
            saved_goods=goods_saved,
            saved_equipment=equipment_saved,
            prodclass_id=prodclass_id,
            prodclass_score=prodclass_score,
            external_request=request_payload_for_response,
            external_status=response.status_code,
            external_response=response_json,
        )
        runs.append(run)

    first_run = runs[0]
    log.info(
        "analyze-json: completed processing inn=%s → runs=%s, total_goods=%s, total_equipment=%s",
        inn,
        len(runs),
        total_saved_goods,
        total_saved_equipment,
    )

    return AnalyzeFromInnResponse(
        status="ok",
        inn=inn,
        pars_id=first_run.pars_id if first_run else None,
        company_id=first_run.company_id if first_run else None,
        text_length=first_run.text_length if first_run else 0,
        catalog_goods_size=first_run.catalog_goods_size if first_run else len(goods_catalog),
        catalog_equipment_size=first_run.catalog_equipment_size if first_run else len(equipment_catalog),
        saved_goods=first_run.saved_goods if first_run else 0,
        saved_equipment=first_run.saved_equipment if first_run else 0,
        prodclass_id=first_run.prodclass_id,
        prodclass_score=first_run.prodclass_score,
        external_request=first_run.external_request if first_run else None,
        external_status=first_run.external_status if first_run else 0,
        external_response=first_run.external_response,
        total_text_length=total_text_length,
        total_saved_goods=total_saved_goods,
        total_saved_equipment=total_saved_equipment,
        domains_processed=domains_processed,
        runs=runs,
    )


@router.post(
    "/analyze-json",
    response_model=AnalyzeFromInnResponse,
    responses={
        400: {"model": AnalyzeFromInnError},
        404: {"model": AnalyzeFromInnError},
        502: {"model": AnalyzeFromInnError},
    },
)
async def analyze_from_inn(payload: AnalyzeFromInnRequest) -> AnalyzeFromInnResponse:
    """Полный цикл анализа через POST-запрос."""

    return await _run_analyze(payload, source="POST")


@router.get(
    "/analyze-json/{inn}",
    response_model=AnalyzeFromInnResponse,
    responses={
        400: {"model": AnalyzeFromInnError},
        404: {"model": AnalyzeFromInnError},
        502: {"model": AnalyzeFromInnError},
    },
)
async def analyze_from_inn_get(
    inn: str,
    refresh_site: bool = False,
    include_catalogs: bool = True,
    chat_model: Optional[str] = None,
    embed_model: Optional[str] = None,
    return_prompt: Optional[bool] = None,
    return_answer_raw: Optional[bool] = None,
) -> AnalyzeFromInnResponse:
    """Упрощённый запуск анализа по ИНН через GET-запрос."""

    query_context = {
        "inn": inn,
        "refresh_site": refresh_site,
        "include_catalogs": include_catalogs,
        "chat_model": chat_model,
        "embed_model": embed_model,
        "return_prompt": return_prompt,
        "return_answer_raw": return_answer_raw,
    }

    try:
        payload = AnalyzeFromInnRequest(
            inn=inn,
            refresh_site=refresh_site,
            include_catalogs=include_catalogs,
            chat_model=chat_model,
            embed_model=embed_model,
            return_prompt=return_prompt,
            return_answer_raw=return_answer_raw,
        )
    except ValidationError as exc:
        def _fmt(err: Mapping[str, Any]) -> str:
            location = "->".join(str(part) for part in err.get("loc", ())) or "payload"
            message = err.get("msg", "unknown error")
            return f"{location} → {message}"

        detail = "Некорректные параметры запроса: " + "; ".join(_fmt(err) for err in exc.errors())
        _log_and_raise(
            status.HTTP_400_BAD_REQUEST,
            inn=(inn or "").strip(),
            detail=detail,
            request_context=query_context,
            level=logging.WARNING,
        )

    return await _run_analyze(payload, source="GET")
