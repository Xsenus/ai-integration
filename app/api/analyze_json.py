from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict
from difflib import SequenceMatcher
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence

import httpx
from fastapi import APIRouter, HTTPException, status
from pydantic import ValidationError
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.sqltypes import Text

from app.services.analyze_client import (
    AnalyzeServiceUnavailable,
    ensure_service_available,
)
from app.services.parse_site import ParseSiteRequest, run_parse_site
from app.config import settings
from app.db.bitrix import bitrix_session
from app.db.parsing import _normalize_domain
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

_TABLE_COLUMNS_CACHE: dict[str, set[str]] = {}

_TEXT_PREVIEW_LIMIT = 400
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
_LARGE_VECTOR_KEYS = {
    "description_vector",
    "text_vector",
    "vector",
    "vectors",
    "embedding",
    "embeddings",
}
_LARGE_TEXT_KEYS = {
    "text_par",
    "prompt",
    "prompt_raw",
    "answer_raw",
    "raw_text",
    "text_raw",
    "chunks",
    "text_chunks",
    "chunks_raw",
}

_PARSER_ERROR_MARKERS = (
    "Не удалось распарсить секцию",
    "failed to parse section",
    "unable to parse section",
)


_DOMAIN_SPLIT_RE = re.compile(r"[\s,;]+")


def _extract_llm_answer(payload: Any) -> Optional[str]:
    """Достаёт исходный ответ модели из различных блоков ответа сервиса."""

    if isinstance(payload, Mapping):
        answer = payload.get("answer_raw")
        if isinstance(answer, str) and answer.strip():
            return answer.strip()

        parsed = payload.get("parsed")
        if isinstance(parsed, Mapping):
            candidate = parsed.get("LLM_ANSWER") or parsed.get("answer")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        db_payload = payload.get("db_payload")
        if isinstance(db_payload, Mapping):
            candidate = db_payload.get("llm_answer")
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

    return None


def _summarize_external_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Готовит компактное представление тела запроса для логов."""

    summary: dict[str, Any] = {}
    for key, value in payload.items():
        if key == "text_par" and isinstance(value, str):
            summary[key] = {
                "length": len(value),
                "preview": value[:_TEXT_PREVIEW_LIMIT],
            }
        elif key in {"goods_catalog", "equipment_catalog"}:
            items_source: Any = value
            if isinstance(value, Mapping):
                items_source = value.get("items")
            if isinstance(items_source, (list, tuple)):
                summary[key] = {"items": len(items_source)}
            else:
                summary[key] = value
        else:
            summary[key] = value
    return summary


async def _get_table_columns(
    conn: AsyncConnection,
    table_name: str,
    schema: str = "public",
) -> set[str]:
    """Возвращает набор колонок таблицы, кэшируя результат между вызовами."""

    cache_key = f"{schema}.{table_name}"
    cached = _TABLE_COLUMNS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    result = await conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            """
        ),
        {"schema": schema, "table": table_name},
    )
    columns = {row[0] for row in result}
    _TABLE_COLUMNS_CACHE[cache_key] = columns
    return columns


def _sanitize_external_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Удаляет тяжёлые поля (каталоги) из запроса перед сохранением."""

    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if key in {"goods_catalog", "equipment_catalog"}:
            if isinstance(value, Mapping):
                items_source = value.get("items")
                if isinstance(items_source, (list, tuple)):
                    count = len(items_source)
                else:
                    count = value.get("items_count") or 0
            elif isinstance(value, (list, tuple)):
                count = len(value)
            else:
                count = 0
            sanitized[key] = {"items": count, "truncated": True}
            continue
        if key == "text_par" and isinstance(value, str):
            sanitized[key] = {"length": len(value)}
            continue
        sanitized[key] = value
    return sanitized


async def _ensure_column_exists(
    conn: AsyncConnection,
    table_name: str,
    column_name: str,
    column_ddl: str,
    *,
    schema: str = "public",
) -> set[str]:
    """Добавляет недостающую колонку в таблицу и обновляет кэш столбцов."""

    columns = await _get_table_columns(conn, table_name, schema)
    if column_name in columns:
        return columns

    log.info(
        "analyze-json: adding missing column %s.%s.%s (%s)",
        schema,
        table_name,
        column_name,
        column_ddl,
    )
    await conn.execute(
        text(
            f"ALTER TABLE {schema}.{table_name} "
            f"ADD COLUMN IF NOT EXISTS {column_name} {column_ddl}"
        )
    )

    cache_key = f"{schema}.{table_name}"
    _TABLE_COLUMNS_CACHE.pop(cache_key, None)
    return await _get_table_columns(conn, table_name, schema)


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


def _extract_domain_candidates(domain_field: str, raw_value: Any) -> list[str]:
    """Возвращает список доменов, которые стоит попробовать обработать."""

    if raw_value is None:
        return []

    text = str(raw_value).strip()
    if not text:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def _add_candidate(value: Optional[str]) -> None:
        if not value:
            return
        lowered = value.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        candidates.append(value)

    if domain_field == "domain_2":
        parts = _DOMAIN_SPLIT_RE.split(text)
        for part in parts:
            if not part:
                continue
            domain_part = part.strip()
            if not domain_part:
                continue
            if "@" in domain_part:
                domain_part = domain_part.split("@", 1)[-1]
            domain_part = domain_part.strip(" \t\n\r<>\"'()")
            normalized = _normalize_domain(domain_part)
            if normalized:
                _add_candidate(normalized)
            else:
                _add_candidate(domain_part.strip())
    else:
        normalized = _normalize_domain(text)
        if normalized:
            _add_candidate(normalized)
        else:
            _add_candidate(text)

    return candidates


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
            await run_parse_site(payload, session)
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


def _vector_values_to_literal(values: Sequence[float]) -> str:
    """Возвращает строковое представление вектора в формате pgvector."""

    return json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))


def _build_catalog_vector_payload(value: Any) -> Optional[dict[str, Any]]:
    """Формирует структуру CatalogVector c literal и values, если возможно."""

    if value is None:
        return None

    literal: Optional[str] = None
    values: list[float] | None = None

    if isinstance(value, Mapping):
        raw_literal = value.get("literal") or value.get("vec") or value.get("text")
        if isinstance(raw_literal, str):
            candidate = raw_literal.strip()
            literal = candidate or None
        raw_values = value.get("values") or value.get("data") or value.get("vector")
        if isinstance(raw_values, (list, tuple)):
            values = []
            for part in raw_values:
                try:
                    values.append(float(part))
                except Exception:  # noqa: BLE001
                    continue
            if not values:
                values = None
    elif isinstance(value, (list, tuple)):
        values = []
        for part in value:
            try:
                values.append(float(part))
            except Exception:  # noqa: BLE001
                continue
        if not values:
            values = None
    elif isinstance(value, str):
        literal_candidate = value.strip()
        literal = literal_candidate or None
        try:
            parsed = json.loads(value)
        except Exception:  # noqa: BLE001
            parsed = None
        if isinstance(parsed, list):
            values = []
            for part in parsed:
                try:
                    values.append(float(part))
                except Exception:  # noqa: BLE001
                    continue
            if not values:
                values = None
    else:
        try:
            numeric = float(value)
        except Exception:  # noqa: BLE001
            try:
                literal_candidate = str(value).strip()
            except Exception:  # noqa: BLE001
                literal_candidate = ""
            literal = literal_candidate or None
        else:
            values = [numeric]

    if values:
        literal = literal or _vector_values_to_literal(values)
    if literal:
        payload: dict[str, Any] = {"literal": literal}
        if values:
            payload["values"] = values
        return payload
    if values:
        return {"values": values}
    return None


def _prepare_text_for_external(text: str) -> str:
    """Удаляет управляющие символы и лишние пробелы перед отправкой во внешний сервис."""

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = _CONTROL_CHAR_RE.sub(" ", normalized)
    normalized = _MULTI_SPACE_RE.sub(" ", normalized)
    normalized = _MULTI_NEWLINE_RE.sub("\n\n", normalized)
    return normalized.strip()


def _prepare_catalog_payload(
    catalog: Iterable[Mapping[str, Any]]
) -> Optional[dict[str, Any]]:
    """Формирует структуру каталога в формате CatalogItemsPayload."""

    items: list[dict[str, Any]] = []
    for item in catalog:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue

        prepared: dict[str, Any] = {"name": name}
        item_id = item.get("id")
        if item_id is not None:
            try:
                prepared["id"] = int(item_id)
            except Exception:  # noqa: BLE001
                prepared["id"] = item_id

        vec_payload = _build_catalog_vector_payload(item.get("vec"))
        if vec_payload is not None:
            prepared["vec"] = vec_payload

        items.append(prepared)

    if not items:
        return None

    return {"items": items}


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
    processed_domains: set[str] = set()

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
                candidates = _extract_domain_candidates(domain_field, row.get(domain_field))
                if not candidates:
                    continue
                for domain_value in candidates:
                    key = domain_value.lower()
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


def _okved_to_industry_label(okved: Optional[str]) -> Optional[str]:
    if not okved:
        return None
    match = re.search(r"(\d{2})", str(okved))
    if not match:
        return None
    code2 = int(match.group(1))
    if 10 <= code2 <= 12:
        return "Пищевая промышленность"
    if 13 <= code2 <= 15:
        return "Текстиль/одежда"
    if 16 <= code2 <= 18:
        return "Деревообработка/бумага/печать"
    if 19 <= code2 <= 22:
        return "Химия/полимеры"
    if 23 <= code2 <= 25:
        return "Неметаллы/металлы/машиностроение"
    if 26 <= code2 <= 28:
        return "Электроника/оборудование"
    if 29 <= code2 <= 30:
        return "Авто/транспортное машиностроение"
    if 31 <= code2 <= 33:
        return "Прочее производство/ремонт"
    if 35 <= code2 <= 39:
        return "Энергетика/вода/утилизация"
    if 41 <= code2 <= 43:
        return "Строительство"
    if 45 <= code2 <= 47:
        return "Торговля"
    if 49 <= code2 <= 53:
        return "Транспорт и логистика"
    if 55 <= code2 <= 56:
        return "Гостиницы/общепит"
    if 58 <= code2 <= 63:
        return "IT/связь/медиа"
    if 64 <= code2 <= 66:
        return "Финансы/страхование"
    if 68 == code2:
        return "Недвижимость"
    if 69 <= code2 <= 75:
        return "Проф. и научные услуги"
    if 77 <= code2 <= 82:
        return "Адм. услуги"
    if 84 == code2:
        return "Госуправление"
    if 85 == code2:
        return "Образование"
    if 86 <= code2 <= 88:
        return "Здравоохранение/соцуслуги"
    if 90 <= code2 <= 93:
        return "Культура/спорт/развлечения"
    if 94 <= code2 <= 96:
        return "Общественные/личные услуги"
    return None


def _compute_description_okved_score(
    description: Optional[str], okved: Optional[str]
) -> Optional[float]:
    if not description or not okved:
        return None

    industry_label = _okved_to_industry_label(okved) or okved
    if not industry_label:
        return None

    normalized_desc = " ".join(str(description).split()).lower()
    normalized_industry = str(industry_label).strip().lower()
    if not normalized_desc or not normalized_industry:
        return None

    ratio = SequenceMatcher(None, normalized_desc[:2000], normalized_industry).ratio()
    if ratio <= 0:
        return None
    return round(ratio, 4)


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


def _compact_dict(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Удаляет пустые значения из словаря."""

    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "", [], {}, ())
    }


def _sanitize_catalog_items(
    items: Any,
    *,
    name_keys: Iterable[str],
    id_keys: Iterable[str],
    score_keys: Iterable[str],
    text_keys: Iterable[str] = ("text",),
) -> list[dict[str, Any]]:
    """Сокращает описание элементов каталога, убирая векторы и служебные поля."""

    if not isinstance(items, Iterable) or isinstance(items, (str, bytes, bytearray, memoryview)):
        return []

    sanitized_items: list[dict[str, Any]] = []
    for raw_item in items:
        if isinstance(raw_item, str):
            candidate = raw_item.strip()
            if candidate:
                sanitized_items.append({"name": candidate, "text": candidate})
            continue
        if not isinstance(raw_item, Mapping):
            continue
        name_value: Optional[str] = None
        text_value: Optional[str] = None
        for key in text_keys:
            candidate = raw_item.get(key)
            if isinstance(candidate, str) and candidate.strip():
                text_value = candidate.strip()
                break
        for key in name_keys:
            candidate = raw_item.get(key)
            if isinstance(candidate, str) and candidate.strip():
                name_value = candidate.strip()
                break
        if not name_value:
            name_value = text_value
        if not name_value:
            continue

        item_payload: dict[str, Any] = {"name": name_value}
        if text_value:
            item_payload["text"] = text_value
        elif isinstance(raw_item.get("text"), str):
            stripped = raw_item["text"].strip()
            if stripped:
                item_payload["text"] = stripped

        for key in id_keys:
            candidate_id = raw_item.get(key)
            candidate_id = _safe_int(candidate_id)
            if candidate_id is not None:
                item_payload["id"] = candidate_id
                break

        score_value: Optional[float] = None
        for key in score_keys:
            candidate_score = raw_item.get(key)
            score_value = _normalize_score(candidate_score)
            if score_value is not None:
                break
        if score_value is not None:
            item_payload["score"] = score_value

        sanitized_items.append(item_payload)

    return sanitized_items


def _sanitize_db_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Удаляет тяжёлые поля из db_payload перед возвратом клиенту."""

    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if key in _LARGE_VECTOR_KEYS:
            continue
        if key in {"goods_catalog", "equipment_catalog"}:
            continue
        if key in {"goods_types", "goods"}:
            sanitized[key] = _sanitize_catalog_items(
                value,
                name_keys=("name", "goods_type"),
                id_keys=("match_id", "id", "goods_type_id", "goods_type_ID"),
                score_keys=("score", "goods_types_score", "match_score"),
                text_keys=("text", "goods_type"),
            )
            continue
        if key in {"equipment", "equipment_site"}:
            sanitized[key] = _sanitize_catalog_items(
                value,
                name_keys=("name", "equipment"),
                id_keys=("match_id", "id", "equipment_id", "equipment_ID"),
                score_keys=("score", "equipment_score", "match_score"),
                text_keys=("text", "equipment"),
            )
            continue
        if key == "prodclass" and isinstance(value, Mapping):
            candidate_id = _safe_int(
                value.get("id")
                or value.get("prodclass")
                or value.get("prodclass_id")
            )
            candidate_score = _normalize_score(
                value.get("score")
                or value.get("prodclass_score")
            )
            description_okved_score = _normalize_score(
                value.get("description_okved_score")
            )
            description_score = _normalize_score(value.get("description_score"))
            okved_score = _normalize_score(value.get("okved_score"))
            prodclass_by_okved = _safe_int(value.get("prodclass_by_okved"))
            sanitized[key] = _compact_dict(
                {
                    "id": candidate_id,
                    "score": candidate_score,
                    "name": (value.get("name") or value.get("prodclass_name") or "").strip() or None,
                    "description_okved_score": description_okved_score,
                    "description_score": description_score,
                    "okved_score": okved_score,
                    "prodclass_by_okved": prodclass_by_okved,
                }
            )
            continue
        sanitized[key] = value
    return _compact_dict(sanitized)


def _sanitize_external_response(payload: Any) -> Any:
    """Удаляет тяжёлые поля из ответа внешнего сервиса перед возвратом клиенту."""

    if isinstance(payload, Mapping):
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            if key in _LARGE_VECTOR_KEYS:
                continue
            if key in _LARGE_TEXT_KEYS:
                if isinstance(value, str):
                    sanitized[key] = {"length": len(value)}
                elif isinstance(value, (list, tuple)):
                    sanitized[key] = {"items": len(value)}
                continue
            if key == "db_payload" and isinstance(value, Mapping):
                sanitized[key] = _sanitize_db_payload(value)
                continue
            sanitized[key] = _sanitize_external_response(value)
        return _compact_dict(sanitized)
    if isinstance(payload, list):
        return [
            item
            for item in (_sanitize_external_response(item) for item in payload)
            if item not in (None, {}, [])
        ]
    return payload


def _iter_text_fragments(payload: Any) -> Iterable[str]:
    if isinstance(payload, Mapping):
        for value in payload.values():
            yield from _iter_text_fragments(value)
    elif isinstance(payload, (list, tuple, set)):
        for item in payload:
            yield from _iter_text_fragments(item)
    elif isinstance(payload, str):
        text = payload.strip()
        if text:
            yield text


def _has_parser_section_error(response_json: Any, response_text: Optional[str]) -> bool:
    for fragment in _iter_text_fragments(response_json):
        lowered = fragment.lower()
        if any(marker.lower() in lowered for marker in _PARSER_ERROR_MARKERS):
            return True
    if response_text:
        lowered = response_text.lower()
        if any(marker.lower() in lowered for marker in _PARSER_ERROR_MARKERS):
            return True
    return False


def _build_error_response_payload(
    response_json: Any,
    response_text: Optional[str],
    json_error: Optional[Exception],
) -> dict[str, Any]:
    if isinstance(response_json, Mapping):
        sanitized = _sanitize_external_response(response_json)
        payload: dict[str, Any] = sanitized if isinstance(sanitized, dict) else {}
    else:
        payload = {}

    if not payload and response_text:
        payload = {"error": response_text.strip()[:_TEXT_PREVIEW_LIMIT]}

    if json_error is not None:
        payload.setdefault("error", f"response.json() failed: {json_error}")

    if not payload:
        payload = {"error": "empty response"}

    return payload


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
    normalized_goods_payload: list[dict[str, Any]] = []
    normalized_equipment_payload: list[dict[str, Any]] = []

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

    payload_lower = {str(k).lower(): v for k, v in payload.items()}
    prodclass_payload = payload.get("prodclass") if isinstance(payload, Mapping) else None
    prodclass_lower = (
        {str(k).lower(): v for k, v in prodclass_payload.items()}
        if isinstance(prodclass_payload, Mapping)
        else {}
    )

    async with engine.begin() as conn:
        prodclass_row: Optional[dict[str, Any]] = None
        prodclass_stale_ids: list[int] = []
        prodclass_columns = await _get_table_columns(conn, "ai_site_prodclass")
        for column_name, ddl in (
            ("description_okved_score", "NUMERIC(6,4)"),
            ("description_score", "NUMERIC(6,4)"),
            ("okved_score", "NUMERIC(6,4)"),
            ("prodclass_by_okved", "INT"),
        ):
            if column_name not in prodclass_columns:
                prodclass_columns = await _ensure_column_exists(
                    conn,
                    "ai_site_prodclass",
                    column_name,
                    ddl,
                )

        prodclass_has_created_at = "created_at" in prodclass_columns
        prodclass_has_okved_score = "description_okved_score" in prodclass_columns
        prodclass_has_description_score = "description_score" in prodclass_columns
        prodclass_has_okved_score_value = "okved_score" in prodclass_columns
        prodclass_has_okved_fallback = "prodclass_by_okved" in prodclass_columns

        select_cols = "id, prodclass, prodclass_score"
        if prodclass_has_okved_score:
            select_cols += ", description_okved_score"
        if prodclass_has_description_score:
            select_cols += ", description_score"
        if prodclass_has_okved_score_value:
            select_cols += ", okved_score"
        if prodclass_has_okved_fallback:
            select_cols += ", prodclass_by_okved"

        result = await conn.execute(
            text(
                f"""
                SELECT {select_cols}
                FROM public.ai_site_prodclass
                WHERE text_pars_id = :pid
                ORDER BY id ASC
                """
            ),
            {"pid": snapshot.pars_id},
        )
        prodclass_rows = [dict(row) for row in result.mappings().all()]
        prodclass_stale_ids = [row.get("id") for row in prodclass_rows[:-1] if row.get("id") is not None]
        if prodclass_rows:
            prodclass_row = prodclass_rows[-1]

        okved_main: Optional[str] = None
        if snapshot.company_id:
            okved_query = text(
                """
                SELECT okved_main
                FROM public.clients_requests
                WHERE id = :cid
                LIMIT 1
                """
            )
            okved_row = await conn.execute(
                okved_query, {"cid": snapshot.company_id}
            )
            okved_data = okved_row.mappings().first()
            if okved_data:
                okved_main = okved_data.get("okved_main")

        okved_score_payload = _normalize_score(
            payload_lower.get("okved_score")
            or prodclass_lower.get("okved_score")
            or prodclass_lower.get("description_okved_score")
        )
        description_score_payload = _normalize_score(
            payload_lower.get("description_score")
            or prodclass_lower.get("description_score")
        )
        prodclass_by_okved_value = _safe_int(
            payload_lower.get("prodclass_by_okved")
            or prodclass_lower.get("prodclass_by_okved")
        )

        description_okved_score = okved_score_payload
        if prodclass_has_okved_score and description_okved_score is None:
            description_okved_score = _compute_description_okved_score(
                description_value or snapshot.text, okved_main
            )

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
            statement = text(update_sql).bindparams(bindparam("vec", type_=Text()))
            await conn.execute(statement, {"vec": vector_literal, "id": snapshot.pars_id})

        okved_score_to_use = okved_score_payload
        description_score_to_use = description_score_payload
        if prodclass_row is not None:
            if okved_score_to_use is None:
                okved_score_to_use = _normalize_score(prodclass_row.get("okved_score"))
            if description_score_to_use is None:
                description_score_to_use = _normalize_score(
                    prodclass_row.get("description_score")
                )
            if prodclass_by_okved_value is None:
                prodclass_by_okved_value = _safe_int(
                    prodclass_row.get("prodclass_by_okved")
                )

        okved_score_param = okved_score_to_use
        if okved_score_param is None and description_okved_score is not None:
            okved_score_param = _normalize_score(description_okved_score)

        raw_prod = payload.get("prodclass")
        candidate_prodclass_id = None
        candidate_prodclass_score = None
        prodclass_source = "prodclass"
        if isinstance(raw_prod, dict):
            candidate_prodclass_id = _safe_int(
                raw_prod.get("id")
                or raw_prod.get("prodclass")
                or raw_prod.get("prodclass_id")
            )
            candidate_prodclass_score = _normalize_score(
                raw_prod.get("score") or raw_prod.get("prodclass_score")
            )
        elif raw_prod is None and prodclass_by_okved_value is not None:
            candidate_prodclass_id = prodclass_by_okved_value
            candidate_prodclass_score = okved_score_to_use or description_okved_score
            prodclass_source = "prodclass_by_okved"
        elif raw_prod is not None:
            log.warning(
                "analyze-json: unexpected prodclass payload type (%s) — keeping existing value (pars_id=%s)",
                type(raw_prod),
                snapshot.pars_id,
            )

        if (
            candidate_prodclass_id is None
            and prodclass_by_okved_value is not None
            and prodclass_row is None
        ):
            candidate_prodclass_id = prodclass_by_okved_value
            candidate_prodclass_score = okved_score_to_use or description_okved_score
            prodclass_source = "prodclass_by_okved"

        if candidate_prodclass_id is not None:
            prodclass_exists = await _prodclass_exists(conn, candidate_prodclass_id)
            if not prodclass_exists:
                log.warning(
                    "analyze-json: ib_prodclass is missing entry id=%s (pars_id=%s) — skipping prodclass write to avoid FK error",
                    candidate_prodclass_id,
                    snapshot.pars_id,
                )
                if prodclass_by_okved_value is None:
                    prodclass_by_okved_value = candidate_prodclass_id
                candidate_prodclass_id = None
                candidate_prodclass_score = None

            score_to_use = candidate_prodclass_score
            if prodclass_row is not None and score_to_use is None:
                score_to_use = _normalize_score(prodclass_row.get("prodclass_score"))

            if candidate_prodclass_id is None:
                score_to_use = None

            params = {
                "pid": snapshot.pars_id,
                "description_okved_score": description_okved_score,
                "description_score": description_score_to_use,
                "okved_score": okved_score_param,
                "prodclass_by_okved": prodclass_by_okved_value,
            }
            if candidate_prodclass_id is not None:
                params["prodclass"] = candidate_prodclass_id
                params["score"] = score_to_use

            if prodclass_row is None:
                columns = ["text_pars_id"]
                values = [":pid"]
                if candidate_prodclass_id is not None:
                    columns.extend(["prodclass", "prodclass_score"])
                    values.extend([":prodclass", ":score"])
                if prodclass_has_okved_score and "description_okved_score" in params:
                    columns.append("description_okved_score")
                    values.append(":description_okved_score")
                if prodclass_has_description_score and "description_score" in params:
                    columns.append("description_score")
                    values.append(":description_score")
                if prodclass_has_okved_score_value and "okved_score" in params:
                    columns.append("okved_score")
                    values.append(":okved_score")
                if prodclass_has_okved_fallback and "prodclass_by_okved" in params:
                    columns.append("prodclass_by_okved")
                    values.append(":prodclass_by_okved")

                if len(columns) > 1:
                    insert_prodclass_sql = (
                        "INSERT INTO public.ai_site_prodclass "
                        f"({', '.join(columns)}) "
                        f"VALUES ({', '.join(values)}) RETURNING id"
                    )
                    log.info(
                        "analyze-json: inserting prodclass (pars_id=%s, prodclass=%s, score=%s, desc_okved=%s, source=%s) using SQL: %s",
                        snapshot.pars_id,
                        candidate_prodclass_id,
                        score_to_use,
                        description_okved_score,
                        prodclass_source,
                        insert_prodclass_sql,
                    )
                    insert_result = await conn.execute(
                        text(insert_prodclass_sql),
                        params,
                    )
                    prodclass_row = {
                        "id": insert_result.scalar_one(),
                        "prodclass": candidate_prodclass_id,
                        "prodclass_score": score_to_use,
                        "description_okved_score": description_okved_score,
                        "description_score": description_score_to_use,
                        "okved_score": okved_score_param,
                        "prodclass_by_okved": prodclass_by_okved_value,
                    }
            else:
                set_clauses: list[str] = []
                if candidate_prodclass_id is not None:
                    set_clauses.extend(["prodclass = :prodclass", "prodclass_score = :score"])
                if prodclass_has_okved_score:
                    set_clauses.append("description_okved_score = :description_okved_score")
                if prodclass_has_description_score:
                    set_clauses.append("description_score = :description_score")
                if prodclass_has_okved_score_value:
                    set_clauses.append("okved_score = :okved_score")
                if prodclass_has_okved_fallback:
                    set_clauses.append("prodclass_by_okved = :prodclass_by_okved")

                if set_clauses:
                    update_prodclass_sql = (
                        "UPDATE public.ai_site_prodclass " f"SET {', '.join(set_clauses)}"
                    )
                    if prodclass_has_created_at:
                        update_prodclass_sql += ", created_at = TIMEZONE('UTC', now())"
                    update_prodclass_sql += " WHERE id = :row_id"
                    log.info(
                        "analyze-json: updating prodclass (pars_id=%s, prodclass=%s, score=%s, desc_okved=%s, source=%s) using SQL: %s",
                        snapshot.pars_id,
                        candidate_prodclass_id,
                        score_to_use,
                        description_okved_score,
                        prodclass_source,
                        update_prodclass_sql,
                    )
                    await conn.execute(
                        text(update_prodclass_sql),
                        {
                            **params,
                            "row_id": prodclass_row.get("id"),
                        },
                    )
                    prodclass_row = {
                        "id": prodclass_row.get("id"),
                        "prodclass": candidate_prodclass_id,
                        "prodclass_score": score_to_use,
                        "description_okved_score": description_okved_score,
                        "description_score": description_score_to_use,
                        "okved_score": okved_score_param,
                        "prodclass_by_okved": prodclass_by_okved_value,
                    }

        elif prodclass_row is not None and prodclass_has_okved_fallback:
            updated_clauses: list[str] = []
            params: dict[str, Any] = {"row_id": prodclass_row.get("id")}

            if (
                prodclass_has_okved_fallback
                and prodclass_by_okved_value is not None
                and _safe_int(prodclass_row.get("prodclass_by_okved"))
                != prodclass_by_okved_value
            ):
                updated_clauses.append("prodclass_by_okved = :prodclass_by_okved")
                params["prodclass_by_okved"] = prodclass_by_okved_value

            if (
                prodclass_has_okved_score
                and description_okved_score is not None
                and _normalize_score(prodclass_row.get("description_okved_score"))
                != _normalize_score(description_okved_score)
            ):
                updated_clauses.append("description_okved_score = :description_okved_score")
                params["description_okved_score"] = description_okved_score

            if (
                prodclass_has_description_score
                and description_score_to_use is not None
                and _normalize_score(prodclass_row.get("description_score"))
                != _normalize_score(description_score_to_use)
            ):
                updated_clauses.append("description_score = :description_score")
                params["description_score"] = description_score_to_use

            if (
                prodclass_has_okved_score_value
                and okved_score_param is not None
                and _normalize_score(prodclass_row.get("okved_score"))
                != _normalize_score(okved_score_param)
            ):
                updated_clauses.append("okved_score = :okved_score")
                params["okved_score"] = okved_score_param

            if updated_clauses:
                update_prodclass_sql = (
                    "UPDATE public.ai_site_prodclass "
                    f"SET {', '.join(updated_clauses)} WHERE id = :row_id"
                )
                log.info(
                    "analyze-json: updating prodclass metrics (pars_id=%s, prodclass_by_okved=%s) using SQL: %s",
                    snapshot.pars_id,
                    prodclass_by_okved_value,
                    update_prodclass_sql,
                )
                await conn.execute(text(update_prodclass_sql), params)
                prodclass_row = {
                    **(prodclass_row or {}),
                    "prodclass_by_okved": params.get("prodclass_by_okved", prodclass_row.get("prodclass_by_okved")),
                    "description_okved_score": params.get("description_okved_score", prodclass_row.get("description_okved_score")),
                    "description_score": params.get("description_score", prodclass_row.get("description_score")),
                    "okved_score": params.get("okved_score", prodclass_row.get("okved_score")),
                }
        if prodclass_row is not None:
            prodclass_id = _safe_int(prodclass_row.get("prodclass"))
            prodclass_score = _normalize_score(prodclass_row.get("prodclass_score"))

        if prodclass_stale_ids:
            delete_sql = "DELETE FROM public.ai_site_prodclass WHERE id = :row_id"
            for stale_id in prodclass_stale_ids:
                log.info(
                    "analyze-json: deleting stale prodclass row (pars_id=%s, row_id=%s) using SQL: %s",
                    snapshot.pars_id,
                    stale_id,
                    delete_sql,
                )
                await conn.execute(text(delete_sql), {"row_id": stale_id})

        if "goods_types" in payload:
            goods_items = payload.get("goods_types")
            normalized_goods: list[dict[str, Any]] = []
            if isinstance(goods_items, list):
                for item in goods_items:
                    if not isinstance(item, dict):
                        continue
                    name = (
                        item.get("name")
                        or item.get("goods_type")
                        or item.get("text")
                        or ""
                    ).strip()
                    if not name:
                        continue
                    match_id = _safe_int(
                        item.get("match_id") or item.get("id") or item.get("goods_type_ID")
                    )
                    score = _normalize_score(item.get("match_score") or item.get("score"))
                    vec_literal = _vector_to_literal(item.get("vector") or item.get("text_vector"))
                    normalized_goods.append(
                        {
                            "name": name,
                            "match_id": match_id,
                            "score": score,
                            "vec": vec_literal,
                        }
                    )

            normalized_goods_payload = normalized_goods

            if not normalized_goods:
                log.info(
                    "analyze-json: goods payload empty — existing rows remain untouched (pars_id=%s)",
                    snapshot.pars_id,
                )
            else:
                result = await conn.execute(
                    text(
                        """
                        SELECT id, goods_type, goods_type_ID AS goods_type_id, goods_types_score
                        FROM public.ai_site_goods_types
                        WHERE text_par_id = :pid
                        ORDER BY id
                        """
                    ),
                    {"pid": snapshot.pars_id},
                )
                existing_goods = [dict(row) for row in result.mappings().all()]
                rows_by_pk: dict[int, dict[str, Any]] = {
                    row["id"]: row for row in existing_goods if row.get("id") is not None
                }
                existing_ids = set(rows_by_pk)

                goods_by_id: dict[int, list[int]] = defaultdict(list)
                goods_by_name: dict[str, list[int]] = defaultdict(list)
                for row in existing_goods:
                    row_id = row.get("id")
                    if row_id is None:
                        continue
                    match_id = row.get("goods_type_id")
                    if match_id is not None:
                        goods_by_id[match_id].append(row_id)
                    name_key = (row.get("goods_type") or "").strip().lower()
                    if name_key:
                        goods_by_name[name_key].append(row_id)

                def consume_goods_row(row_id: int) -> Optional[dict[str, Any]]:
                    row = rows_by_pk.get(row_id)
                    if row is None:
                        return None

                    match_id_val = row.get("goods_type_id")
                    if match_id_val is not None:
                        id_candidates = goods_by_id.get(match_id_val)
                        if id_candidates:
                            try:
                                id_candidates.remove(row_id)
                            except ValueError:
                                pass
                            if not id_candidates:
                                goods_by_id.pop(match_id_val, None)

                    name_key_val = (row.get("goods_type") or "").strip().lower()
                    if name_key_val:
                        name_candidates = goods_by_name.get(name_key_val)
                        if name_candidates:
                            try:
                                name_candidates.remove(row_id)
                            except ValueError:
                                pass
                            if not name_candidates:
                                goods_by_name.pop(name_key_val, None)

                    return row

                def pop_goods_by_id(match_id_val: int, normalized_name: str) -> Optional[dict[str, Any]]:
                    candidates = goods_by_id.get(match_id_val)
                    if not candidates:
                        return None

                    selected_row_id: Optional[int] = None
                    if normalized_name:
                        for idx, candidate_row_id in enumerate(list(candidates)):
                            candidate_row = rows_by_pk.get(candidate_row_id)
                            if candidate_row is None:
                                continue
                            candidate_name = (candidate_row.get("goods_type") or "").strip().lower()
                            if candidate_name == normalized_name:
                                selected_row_id = candidates.pop(idx)
                                break

                    if selected_row_id is None:
                        selected_row_id = candidates.pop(0)

                    if not candidates:
                        goods_by_id.pop(match_id_val, None)

                    return consume_goods_row(selected_row_id)

                def pop_goods_by_name(normalized_name: str) -> Optional[dict[str, Any]]:
                    if not normalized_name:
                        return None

                    candidates = goods_by_name.get(normalized_name)
                    if not candidates:
                        return None

                    row_id = candidates.pop(0)
                    if not candidates:
                        goods_by_name.pop(normalized_name, None)

                    return consume_goods_row(row_id)

                log.info(
                    "analyze-json: syncing goods payload (pars_id=%s, incoming=%s, existing=%s)",
                    snapshot.pars_id,
                    len(normalized_goods),
                    len(existing_goods),
                )

                insert_sql = text(
                    """
                    INSERT INTO public.ai_site_goods_types
                        (text_par_id, goods_type, goods_types_score, goods_type_ID, text_vector)
                    VALUES
                        (:pid, :name, :score, :match_id,
                         CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END)
                    """
                ).bindparams(bindparam("vec", type_=Text()))
                update_sql_with_vec = text(
                    """
                    UPDATE public.ai_site_goods_types
                    SET goods_type = :name,
                        goods_types_score = :score,
                        goods_type_ID = :match_id,
                        text_vector = CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END
                    WHERE id = :row_id
                    """
                ).bindparams(bindparam("vec", type_=Text()))
                update_sql_no_vec = text(
                    """
                    UPDATE public.ai_site_goods_types
                    SET goods_type = :name,
                        goods_types_score = :score,
                        goods_type_ID = :match_id
                    WHERE id = :row_id
                    """
                )

                processed_ids: set[int] = set()
                for item in normalized_goods:
                    name = item["name"]
                    name_key = name.lower()
                    match_id = item["match_id"]
                    score = item["score"]
                    vec_literal = item["vec"]

                    existing_row: Optional[dict[str, Any]] = None
                    if match_id is not None:
                        existing_row = pop_goods_by_id(match_id, name_key)
                    if existing_row is None:
                        existing_row = pop_goods_by_name(name_key)

                    score_to_use = score
                    if existing_row is not None:
                        processed_ids.add(existing_row["id"])
                        if score_to_use is None:
                            score_to_use = _normalize_score(existing_row.get("goods_types_score"))
                        statement = update_sql_with_vec if vec_literal is not None else update_sql_no_vec
                        params = {
                            "row_id": existing_row["id"],
                            "name": name,
                            "score": score_to_use,
                            "match_id": match_id,
                        }
                        if vec_literal is not None:
                            params["vec"] = vec_literal
                        log.info(
                            "analyze-json: updating goods_type (pars_id=%s, row_id=%s, name=%s, match_id=%s, score=%s)",
                            snapshot.pars_id,
                            existing_row["id"],
                            name,
                            match_id,
                            score_to_use,
                        )
                        await conn.execute(statement, params)
                    else:
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

                leftover_ids = existing_ids - processed_ids
                for row_id in leftover_ids:
                    log.info(
                        "analyze-json: deleting stale goods_type row (pars_id=%s, row_id=%s)",
                        snapshot.pars_id,
                        row_id,
                    )
                    await conn.execute(
                        text("DELETE FROM public.ai_site_goods_types WHERE id = :row_id"),
                        {"row_id": row_id},
                    )

        if "equipment" in payload:
            equipment_items = payload.get("equipment")
            normalized_equipment: list[dict[str, Any]] = []
            if isinstance(equipment_items, list):
                for item in equipment_items:
                    if not isinstance(item, dict):
                        continue
                    name = (
                        item.get("name")
                        or item.get("equipment")
                        or item.get("text")
                        or ""
                    ).strip()
                    if not name:
                        continue
                    match_id = _safe_int(
                        item.get("match_id") or item.get("id") or item.get("equipment_ID")
                    )
                    score = _normalize_score(item.get("match_score") or item.get("score"))
                    vec_literal = _vector_to_literal(item.get("vector") or item.get("text_vector"))
                    normalized_equipment.append(
                        {
                            "name": name,
                            "match_id": match_id,
                            "score": score,
                            "vec": vec_literal,
                        }
                    )

            normalized_equipment_payload = normalized_equipment

            if not normalized_equipment:
                log.info(
                    "analyze-json: equipment payload empty — existing rows remain untouched (pars_id=%s)",
                    snapshot.pars_id,
                )
            else:
                result = await conn.execute(
                    text(
                        """
                        SELECT id, equipment, equipment_ID AS equipment_id, equipment_score
                        FROM public.ai_site_equipment
                        WHERE text_pars_id = :pid
                        ORDER BY id
                        """
                    ),
                    {"pid": snapshot.pars_id},
                )
                existing_equipment = [dict(row) for row in result.mappings().all()]
                equipment_rows_by_pk: dict[int, dict[str, Any]] = {
                    row["id"]: row for row in existing_equipment if row.get("id") is not None
                }
                existing_ids = set(equipment_rows_by_pk)

                equipment_by_id: dict[int, list[int]] = defaultdict(list)
                equipment_by_name: dict[str, list[int]] = defaultdict(list)
                for row in existing_equipment:
                    row_id = row.get("id")
                    if row_id is None:
                        continue
                    match_id = row.get("equipment_id")
                    if match_id is not None:
                        equipment_by_id[match_id].append(row_id)
                    name_key = (row.get("equipment") or "").strip().lower()
                    if name_key:
                        equipment_by_name[name_key].append(row_id)

                def consume_equipment_row(row_id: int) -> Optional[dict[str, Any]]:
                    row = equipment_rows_by_pk.get(row_id)
                    if row is None:
                        return None

                    match_id_val = row.get("equipment_id")
                    if match_id_val is not None:
                        id_candidates = equipment_by_id.get(match_id_val)
                        if id_candidates:
                            try:
                                id_candidates.remove(row_id)
                            except ValueError:
                                pass
                            if not id_candidates:
                                equipment_by_id.pop(match_id_val, None)

                    name_key_val = (row.get("equipment") or "").strip().lower()
                    if name_key_val:
                        name_candidates = equipment_by_name.get(name_key_val)
                        if name_candidates:
                            try:
                                name_candidates.remove(row_id)
                            except ValueError:
                                pass
                            if not name_candidates:
                                equipment_by_name.pop(name_key_val, None)

                    return row

                def pop_equipment_by_id(match_id_val: int, normalized_name: str) -> Optional[dict[str, Any]]:
                    candidates = equipment_by_id.get(match_id_val)
                    if not candidates:
                        return None

                    selected_row_id: Optional[int] = None
                    if normalized_name:
                        for idx, candidate_row_id in enumerate(list(candidates)):
                            candidate_row = equipment_rows_by_pk.get(candidate_row_id)
                            if candidate_row is None:
                                continue
                            candidate_name = (candidate_row.get("equipment") or "").strip().lower()
                            if candidate_name == normalized_name:
                                selected_row_id = candidates.pop(idx)
                                break

                    if selected_row_id is None:
                        selected_row_id = candidates.pop(0)

                    if not candidates:
                        equipment_by_id.pop(match_id_val, None)

                    return consume_equipment_row(selected_row_id)

                def pop_equipment_by_name(normalized_name: str) -> Optional[dict[str, Any]]:
                    if not normalized_name:
                        return None

                    candidates = equipment_by_name.get(normalized_name)
                    if not candidates:
                        return None

                    row_id = candidates.pop(0)
                    if not candidates:
                        equipment_by_name.pop(normalized_name, None)

                    return consume_equipment_row(row_id)

                log.info(
                    "analyze-json: syncing equipment payload (pars_id=%s, incoming=%s, existing=%s)",
                    snapshot.pars_id,
                    len(normalized_equipment),
                    len(existing_equipment),
                )

                insert_sql = text(
                    """
                    INSERT INTO public.ai_site_equipment
                        (text_pars_id, equipment, equipment_score, equipment_ID, text_vector)
                    VALUES
                        (:pid, :name, :score, :match_id,
                         CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END)
                    """
                ).bindparams(bindparam("vec", type_=Text()))
                update_sql_with_vec = text(
                    """
                    UPDATE public.ai_site_equipment
                    SET equipment = :name,
                        equipment_score = :score,
                        equipment_ID = :match_id,
                        text_vector = CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END
                    WHERE id = :row_id
                    """
                ).bindparams(bindparam("vec", type_=Text()))
                update_sql_no_vec = text(
                    """
                    UPDATE public.ai_site_equipment
                    SET equipment = :name,
                        equipment_score = :score,
                        equipment_ID = :match_id
                    WHERE id = :row_id
                    """
                )

                processed_ids: set[int] = set()
                for item in normalized_equipment:
                    name = item["name"]
                    name_key = name.lower()
                    match_id = item["match_id"]
                    score = item["score"]
                    vec_literal = item["vec"]

                    existing_row: Optional[dict[str, Any]] = None
                    if match_id is not None:
                        existing_row = pop_equipment_by_id(match_id, name_key)
                    if existing_row is None:
                        existing_row = pop_equipment_by_name(name_key)

                    score_to_use = score
                    if existing_row is not None:
                        processed_ids.add(existing_row["id"])
                        if score_to_use is None:
                            score_to_use = _normalize_score(existing_row.get("equipment_score"))
                        statement = update_sql_with_vec if vec_literal is not None else update_sql_no_vec
                        params = {
                            "row_id": existing_row["id"],
                            "name": name,
                            "score": score_to_use,
                            "match_id": match_id,
                        }
                        if vec_literal is not None:
                            params["vec"] = vec_literal
                        log.info(
                            "analyze-json: updating equipment (pars_id=%s, row_id=%s, name=%s, match_id=%s, score=%s)",
                            snapshot.pars_id,
                            existing_row["id"],
                            name,
                            match_id,
                            score_to_use,
                        )
                        await conn.execute(statement, params)
                    else:
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

                leftover_ids = existing_ids - processed_ids
                for row_id in leftover_ids:
                    log.info(
                        "analyze-json: deleting stale equipment row (pars_id=%s, row_id=%s)",
                        snapshot.pars_id,
                        row_id,
                    )
                    await conn.execute(
                        text("DELETE FROM public.ai_site_equipment WHERE id = :row_id"),
                        {"row_id": row_id},
                    )

        goods_values = [item.get("name") for item in normalized_goods_payload if item.get("name")]
        equipment_values = [
            item.get("name") for item in normalized_equipment_payload if item.get("name")
        ]
        goods_type_values = [
            {
                "name": item.get("name"),
                "match_id": item.get("match_id"),
                "score": item.get("score"),
            }
            for item in normalized_goods_payload
            if item.get("name")
        ]

        prodclass_row_prodclass = _safe_int(prodclass_row.get("prodclass")) if prodclass_row else None
        prodclass_row_score = (
            _normalize_score(prodclass_row.get("prodclass_score")) if prodclass_row else None
        )

        try:
            await conn.execute(
                text(
                    """
                    INSERT INTO public.ai_site_openai_responses (
                        text_pars_id, company_id, domain, url,
                        description, description_score, okved_score, prodclass_by_okved,
                        prodclass, prodclass_score, equipment_site, goods, goods_type,
                        created_at
                    )
                    VALUES (
                        :text_pars_id, :company_id, :domain, :url,
                        :description, :description_score, :okved_score, :prodclass_by_okved,
                        :prodclass, :prodclass_score, :equipment_site, :goods, :goods_type,
                        now()
                    )
                    """
                ),
                {
                    "text_pars_id": snapshot.pars_id,
                    "company_id": snapshot.company_id,
                    "domain": snapshot.domain,
                    "url": snapshot.url,
                    "description": description_value,
                    "description_score": description_score_to_use,
                    "okved_score": okved_score_param,
                    "prodclass_by_okved": prodclass_by_okved_value,
                    "prodclass": prodclass_row_prodclass,
                    "prodclass_score": prodclass_row_score,
                    "equipment_site": equipment_values or None,
                    "goods": goods_values or None,
                    "goods_type": goods_type_values or None,
                },
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "analyze-json: failed to store full openai response (pars_id=%s): %s",
                snapshot.pars_id,
                exc,
            )

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

    try:
        await ensure_service_available(base_url, label=f"analyze-json:{inn}")
    except AnalyzeServiceUnavailable as exc:
        _log_and_raise(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            inn=inn,
            detail=str(exc),
            request_context=payload_data,
            level=logging.WARNING,
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

    goods_catalog_items: list[dict[str, Any]] = []
    equipment_catalog_items: list[dict[str, Any]] = []
    if payload.include_catalogs:
        try:
            goods_catalog_items = await _load_catalog(
                engine,
                table=settings.IB_GOODS_TYPES_TABLE,
                id_col=settings.IB_GOODS_TYPES_ID_COLUMN,
                name_col=settings.IB_GOODS_TYPES_NAME_COLUMN,
                vec_col=settings.IB_GOODS_TYPES_VECTOR_COLUMN,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("analyze-json: failed to load goods catalog: %s", exc)
            goods_catalog_items = []
        try:
            equipment_catalog_items = await _load_catalog(
                engine,
                table=settings.IB_EQUIPMENT_TABLE,
                id_col=settings.IB_EQUIPMENT_ID_COLUMN,
                name_col=settings.IB_EQUIPMENT_NAME_COLUMN,
                vec_col=settings.IB_EQUIPMENT_VECTOR_COLUMN,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("analyze-json: failed to load equipment catalog: %s", exc)
            equipment_catalog_items = []
    else:
        log.info("analyze-json: catalogs loading skipped by request")

    base_endpoint = base_url.rstrip("/")
    if base_endpoint.endswith("/v1/analyze/json"):
        external_url = base_endpoint
    else:
        external_url = f"{base_endpoint}/v1/analyze/json"
    log.info("analyze-json: external endpoint resolved → %s", external_url)
    client = await _get_http_client()

    runs: list[AnalyzeFromInnRun] = []
    total_text_length = 0
    total_saved_goods = 0
    total_saved_equipment = 0
    successful_runs = 0
    failure_messages: list[str] = []
    domains_processed: list[str] = []

    goods_catalog_payload = _prepare_catalog_payload(goods_catalog_items)
    equipment_catalog_payload = _prepare_catalog_payload(equipment_catalog_items)

    for idx, snapshot in enumerate(snapshots, start=1):
        domain_label = snapshot.domain or ""
        domain_source = snapshot.domain_source or ("domain_1" if domain_label else "auto")
        domains_processed.append(domain_label or "(unknown)")

        prepared_text = _prepare_text_for_external(snapshot.text)
        if not prepared_text:
            _log_and_raise(
                status.HTTP_502_BAD_GATEWAY,
                inn=inn,
                detail="Текст pars_site пустой после нормализации",
                request_context=payload_data,
                extra={
                    "snapshot": {
                        "pars_id": snapshot.pars_id,
                        "company_id": snapshot.company_id,
                        "domain": domain_label or None,
                        "domain_source": domain_source,
                        "chunks": len(snapshot.chunk_ids),
                    }
                },
            )

        raw_text_length = len(snapshot.text)
        text_length = len(prepared_text)
        snapshot_context = {
            "pars_id": snapshot.pars_id,
            "company_id": snapshot.company_id,
            "domain": domain_label or None,
            "domain_source": domain_source,
            "chunks": len(snapshot.chunk_ids),
            "text_length": text_length,
            "text_length_raw": raw_text_length,
        }
        log.info(
            "analyze-json: preparing request #%s (inn=%s, snapshot=%s)",
            idx,
            inn,
            snapshot_context,
        )

        request_payload: dict[str, Any] = {
            "pars_id": snapshot.pars_id,
            "text_par": prepared_text,
        }
        if snapshot.company_id is not None:
            request_payload["company_id"] = snapshot.company_id
        if payload.chat_model:
            request_payload["chat_model"] = payload.chat_model
        elif settings.CHAT_MODEL:
            request_payload["chat_model"] = settings.CHAT_MODEL
        if payload.embed_model:
            request_payload["embed_model"] = payload.embed_model
        elif settings.embed_model:
            request_payload["embed_model"] = settings.embed_model
        if payload.return_prompt is not None:
            request_payload["return_prompt"] = payload.return_prompt
        if payload.return_answer_raw is not None:
            request_payload["return_answer_raw"] = payload.return_answer_raw
        if goods_catalog_payload:
            request_payload["goods_catalog"] = goods_catalog_payload

        if equipment_catalog_payload:
            request_payload["equipment_catalog"] = equipment_catalog_payload

        sanitized_request_payload = _sanitize_external_payload(request_payload)
        payload_summary = _summarize_external_payload(sanitized_request_payload)

        run_url = external_url
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

        response_text = response.text
        response_json: Any | None = None
        json_error: Exception | None = None
        try:
            response_json = response.json()
        except Exception as exc:  # noqa: BLE001
            json_error = exc

        sanitized_external_response = (
            _sanitize_external_response(response_json)
            if isinstance(response_json, Mapping)
            else None
        )

        log.info(
            "analyze-json: response received #%s (inn=%s, domain=%s, status=%s)",
            idx,
            inn,
            domain_label,
            response.status_code,
        )

        if response.status_code >= 500:
            error_payload = _build_error_response_payload(response_json, response_text, json_error)
            error_message = error_payload.get("error") or f"HTTP {response.status_code}"
            failure_messages.append(error_message)
            if _has_parser_section_error(response_json, response_text):
                log.warning(
                    "analyze-json: external parser failed for #%s (inn=%s, domain=%s): %s",
                    idx,
                    inn,
                    domain_label,
                    error_message,
                )
            else:
                log.warning(
                    "analyze-json: external service returned %s for #%s (inn=%s, domain=%s): %s",
                    response.status_code,
                    idx,
                    inn,
                    domain_label,
                    error_message,
                )

            total_text_length += text_length

            run = AnalyzeFromInnRun(
                domain=domain_label or None,
                domain_source=domain_source,
                pars_id=snapshot.pars_id,
                company_id=snapshot.company_id,
                created_at=snapshot.created_at,
                text_length=text_length,
                chunk_count=len(snapshot.chunk_ids),
                catalog_goods_size=len(goods_catalog_items),
                catalog_equipment_size=len(equipment_catalog_items),
                saved_goods=0,
                saved_equipment=0,
                prodclass_id=None,
                prodclass_score=None,
                external_request=sanitized_request_payload,
                external_status=response.status_code,
                external_response=error_payload,
                external_response_raw=response_json if response_json is not None else response_text,
            )
            runs.append(run)
            continue

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
                    "response": response_json if response_json is not None else response_text,
                },
            )

        if json_error is not None:
            detail = f"Не удалось распарсить ответ внешнего сервиса: {json_error}".strip()
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
                    "body": response_text,
                },
            )

        if not isinstance(response_json, Mapping):
            detail = "Ответ внешнего сервиса имеет неожиданный формат"
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
                    "body": response_text,
                },
            )

        if sanitized_external_response is None:
            sanitized_external_response = _build_error_response_payload(response_json, response_text, None)

        log.info(
            "analyze-json: external response summary #%s (inn=%s, domain=%s) → %s",
            idx,
            inn,
            domain_label,
            sanitized_external_response,
        )
        llm_answer = _extract_llm_answer(response_json)
        if llm_answer:
            log.info(
                "analyze-json: external LLM answer #%s (inn=%s, domain=%s, length=%s, preview=%s)",
                idx,
                inn,
                domain_label,
                len(llm_answer),
                llm_answer[:_TEXT_PREVIEW_LIMIT],
            )
        log.debug(
            "analyze-json: external raw response #%s (inn=%s, domain=%s) %s",
            idx,
            inn,
            domain_label,
            response_json,
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
        successful_runs += 1

        run = AnalyzeFromInnRun(
            domain=domain_label or None,
            domain_source=domain_source,
            pars_id=snapshot.pars_id,
            company_id=snapshot.company_id,
            created_at=snapshot.created_at,
            text_length=text_length,
            chunk_count=len(snapshot.chunk_ids),
            catalog_goods_size=len(goods_catalog_items),
            catalog_equipment_size=len(equipment_catalog_items),
            saved_goods=goods_saved,
            saved_equipment=equipment_saved,
            prodclass_id=prodclass_id,
            prodclass_score=prodclass_score,
            external_request=sanitized_request_payload,
            external_status=response.status_code,
            external_response=sanitized_external_response,
            external_response_raw=response_json,
        )
        runs.append(run)

    if not runs:
        _log_and_raise(
            status.HTTP_502_BAD_GATEWAY,
            inn=inn,
            detail="Внешний сервис не вернул результатов",
            request_context=payload_data,
            extra={"domains": domains_processed},
        )

    first_success_run = next((run for run in runs if run.external_status < 400), None)
    first_run = first_success_run or runs[0]

    overall_status = "ok"
    if successful_runs == 0:
        overall_status = "error"
        log.warning(
            "analyze-json: all runs failed for inn=%s (domains=%s, reasons=%s)",
            inn,
            domains_processed,
            failure_messages,
        )
    elif successful_runs < len(runs):
        overall_status = "partial"
        log.warning(
            "analyze-json: partial success for inn=%s (success=%s/%s, failures=%s)",
            inn,
            successful_runs,
            len(runs),
            failure_messages,
        )
    else:
        log.info(
            "analyze-json: completed processing inn=%s → runs=%s, total_goods=%s, total_equipment=%s",
            inn,
            len(runs),
            total_saved_goods,
            total_saved_equipment,
        )

    return AnalyzeFromInnResponse(
        status=overall_status,
        inn=inn,
        pars_id=first_run.pars_id if first_run else None,
        company_id=first_run.company_id if first_run else None,
        text_length=first_run.text_length if first_run else 0,
        catalog_goods_size=first_run.catalog_goods_size if first_run else len(goods_catalog_items),
        catalog_equipment_size=first_run.catalog_equipment_size if first_run else len(equipment_catalog_items),
        saved_goods=first_run.saved_goods if first_run else 0,
        saved_equipment=first_run.saved_equipment if first_run else 0,
        prodclass_id=first_run.prodclass_id,
        prodclass_score=first_run.prodclass_score,
        external_request=first_run.external_request if first_run else None,
        external_status=first_run.external_status if first_run else 0,
        external_response=first_run.external_response,
        external_response_raw=first_run.external_response_raw if first_run else None,
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
