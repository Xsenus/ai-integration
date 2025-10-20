from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx
from sqlalchemy import text

from app.config import settings
from app.db.postgres import get_postgres_engine
from app.services.analyze_client import (
    AnalyzeServiceUnavailable,
    ensure_service_available,
)
from app.services.analyze_health import ensure_analyze_client_health
from app.services.vector_similarity import cosine_similarity

log = logging.getLogger("services.ib_match")

_DEFAULT_EMBED_BASE = "http://37.221.125.221:8123"
_EMBED_ENDPOINT = "/ai-search"
_MAX_BATCH_SIZE = 64
_BORDER_WIDTH = 88

_VECTOR_SOURCE_CATALOG = "catalog"
_VECTOR_SOURCE_NAME = "name_embedding"

_catalog_name_embeddings: Dict[str, List[float]] = {}


class IbMatchServiceError(RuntimeError):
    """Исключение домена IB-match с привязкой HTTP статуса."""

    def __init__(self, message: str, *, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class SourceRow:
    ai_id: int
    text: str
    vector: Optional[List[float]]


@dataclass
class CatalogEntry:
    ib_id: int
    name: str
    vectors: List[List[float]] = field(default_factory=list)
    vector_sources: List[str] = field(default_factory=list)

    def add_vector(self, vector: Sequence[float], *, source: str) -> None:
        """Register a new vector for the catalog entry."""

        values = [float(x) for x in vector]
        if not values:
            return
        self.vectors.append(values)
        self.vector_sources.append(source)

    def has_vectors(self) -> bool:
        return bool(self.vectors)


@dataclass
class MatchResult:
    ai_id: int
    text: str
    match_ib_id: Optional[int]
    match_ib_name: Optional[str]
    score: Optional[float]
    note: str = ""


def _serialize_goods_match(match: MatchResult) -> dict[str, Any]:
    return {
        "ai_goods_id": match.ai_id,
        "ai_goods_type": match.text,
        "match_ib_id": match.match_ib_id,
        "match_ib_name": match.match_ib_name,
        "score": match.score,
        "note": match.note or None,
    }


def _serialize_equipment_match(match: MatchResult) -> dict[str, Any]:
    return {
        "ai_equip_id": match.ai_id,
        "ai_equipment": match.text,
        "match_ib_id": match.match_ib_id,
        "match_ib_name": match.match_ib_name,
        "score": match.score,
        "note": match.note or None,
    }


def _serialize_prodclass_match(match: MatchResult) -> dict[str, Any]:
    return {
        "pars_site_id": match.ai_id,
        "label": match.text,
        "match_ib_id": match.match_ib_id,
        "match_ib_name": match.match_ib_name,
        "score": match.score,
        "note": match.note or None,
    }


async def assign_ib_matches(*, client_id: int, reembed_if_exists: bool) -> dict[str, Any]:
    started_at = time.perf_counter()
    log.info(
        "ib-match: start assignment for client_id=%s (reembed_if_exists=%s)",
        client_id,
        reembed_if_exists,
    )
    engine = get_postgres_engine()
    if engine is None:
        raise IbMatchServiceError("Postgres DSN is not configured", status_code=503)

    async with engine.connect() as conn:
        goods_rows_raw = await conn.execute(
            text(
                """
                SELECT g.id, g.goods_type, g.text_vector::text AS text_vector
                FROM public.ai_site_goods_types AS g
                JOIN public.pars_site AS ps ON ps.id = g.text_par_id
                WHERE ps.company_id = :client_id
                ORDER BY g.id
                """
            ),
            {"client_id": client_id},
        )
        goods_rows = [
            SourceRow(
                ai_id=int(row["id"]),
                text=str(row["goods_type"] or ""),
                vector=_parse_pgvector(row["text_vector"]),
            )
            for row in goods_rows_raw.mappings()
        ]

        equip_rows_raw = await conn.execute(
            text(
                """
                SELECT e.id, e.equipment, e.text_vector::text AS text_vector
                FROM public.ai_site_equipment AS e
                JOIN public.pars_site AS ps ON ps.id = e.text_pars_id
                WHERE ps.company_id = :client_id
                ORDER BY e.id
                """
            ),
            {"client_id": client_id},
        )
        equipment_rows = [
            SourceRow(
                ai_id=int(row["id"]),
                text=str(row["equipment"] or ""),
                vector=_parse_pgvector(row["text_vector"]),
            )
            for row in equip_rows_raw.mappings()
        ]
        log.info(
            "ib-match: loaded %s goods rows and %s equipment rows for client_id=%s",
            len(goods_rows),
            len(equipment_rows),
            client_id,
        )

        pars_rows_raw = await conn.execute(
            text(
                """
                SELECT ps.id, ps.domain_1, ps.url, ps.text_vector::text AS text_vector
                FROM public.pars_site AS ps
                WHERE ps.company_id = :client_id
                ORDER BY ps.id
                """
            ),
            {"client_id": client_id},
        )
        prodclass_rows: List[SourceRow] = []
        for row in pars_rows_raw.mappings():
            vector = _parse_pgvector(row["text_vector"])
            domain = str(row.get("domain_1") or "").strip()
            url = str(row.get("url") or "").strip()
            label_bits = [bit for bit in (domain, url) if bit]
            label = " | ".join(label_bits) if label_bits else f"pars_site:{row['id']}"
            prodclass_rows.append(
                SourceRow(
                    ai_id=int(row["id"]),
                    text=label,
                    vector=vector,
                )
            )
        log.info(
            "ib-match: loaded %s pars_site rows for prodclass scoring (client_id=%s)",
            len(prodclass_rows),
            client_id,
        )

        ib_goods_raw = await conn.execute(
            text(
                """
                SELECT id, goods_type_name, goods_type_vector::text AS vector
                FROM public.{table}
                WHERE {vector} IS NOT NULL
                """.format(
                    table=settings.IB_GOODS_TYPES_TABLE,
                    vector=settings.IB_GOODS_TYPES_VECTOR_COLUMN,
                )
            )
        )
        ib_goods: List[CatalogEntry] = []
        for row in ib_goods_raw.mappings():
            vector = _parse_pgvector(row["vector"])
            if vector is None:
                continue
            entry = CatalogEntry(
                ib_id=int(row["id"]),
                name=str(row[settings.IB_GOODS_TYPES_NAME_COLUMN] or ""),
            )
            entry.add_vector(vector, source=_VECTOR_SOURCE_CATALOG)
            ib_goods.append(entry)

        ib_equipment_raw = await conn.execute(
            text(
                """
                SELECT id, equipment_name, equipment_vector::text AS vector
                FROM public.{table}
                WHERE {vector} IS NOT NULL
                """.format(
                    table=settings.IB_EQUIPMENT_TABLE,
                    vector=settings.IB_EQUIPMENT_VECTOR_COLUMN,
                )
            )
        )
        ib_equipment: List[CatalogEntry] = []
        for row in ib_equipment_raw.mappings():
            vector = _parse_pgvector(row["vector"])
            if vector is None:
                continue
            entry = CatalogEntry(
                ib_id=int(row["id"]),
                name=str(row[settings.IB_EQUIPMENT_NAME_COLUMN] or ""),
            )
            entry.add_vector(vector, source=_VECTOR_SOURCE_CATALOG)
            ib_equipment.append(entry)
        log.info(
            "ib-match: loaded %s ib_goods_types and %s ib_equipment entries",
            len(ib_goods),
            len(ib_equipment),
        )

        ib_prodclass_raw = await conn.execute(
            text(
                """
                SELECT {id_column} AS id,
                       {name_column} AS name,
                       {vector_column}::text AS vector
                FROM public.{table}
                WHERE {vector_column} IS NOT NULL
                """.format(
                    table=settings.IB_PRODCLASS_TABLE,
                    id_column=settings.IB_PRODCLASS_ID_COLUMN,
                    name_column=settings.IB_PRODCLASS_NAME_COLUMN,
                    vector_column=settings.IB_PRODCLASS_VECTOR_COLUMN,
                )
            )
        )
        ib_prodclass: List[CatalogEntry] = []
        for row in ib_prodclass_raw.mappings():
            vector = _parse_pgvector(row["vector"])
            entry = CatalogEntry(
                ib_id=int(row["id"]),
                name=str(row.get("name") or ""),
            )
            if vector is not None:
                entry.add_vector(vector, source=_VECTOR_SOURCE_CATALOG)
            if entry.has_vectors() or entry.name:
                ib_prodclass.append(entry)
        added_name_vectors = await _augment_prodclass_catalog(ib_prodclass)
        vectors_available = sum(1 for entry in ib_prodclass if entry.vectors)
        log.info(
            "ib-match: loaded %s ib_prodclass entries (vectors=%s, name_embeddings_added=%s)",
            len(ib_prodclass),
            vectors_available,
            added_name_vectors,
        )

        existing_prodclass_raw = await conn.execute(
            text(
                """
                SELECT pc.id, pc.text_pars_id
                FROM public.ai_site_prodclass AS pc
                JOIN public.pars_site AS ps ON ps.id = pc.text_pars_id
                WHERE ps.company_id = :client_id
                """
            ),
            {"client_id": client_id},
        )
        prodclass_existing_map = {
            int(row["text_pars_id"]): int(row["id"])
            for row in existing_prodclass_raw.mappings()
        }

    goods_embed_targets = _select_for_embedding(goods_rows, reembed_if_exists)
    equipment_embed_targets = _select_for_embedding(equipment_rows, reembed_if_exists)
    log.info(
        "ib-match: embedding targets for client_id=%s → goods=%s, equipment=%s",
        client_id,
        len(goods_embed_targets),
        len(equipment_embed_targets),
    )

    goods_embeddings_generated = 0
    equipment_embeddings_generated = 0

    if goods_embed_targets:
        log.info("ib-match: generating %s goods embeddings", len(goods_embed_targets))
        goods_vectors = await _embed_and_update_rows(goods_embed_targets)
        goods_embeddings_generated = len(goods_embed_targets)
        for row, vector in zip(goods_embed_targets, goods_vectors):
            row.vector = vector
    else:
        log.info("ib-match: goods embeddings already exist — skipping regeneration")
    if equipment_embed_targets:
        log.info("ib-match: generating %s equipment embeddings", len(equipment_embed_targets))
        equipment_vectors = await _embed_and_update_rows(equipment_embed_targets)
        equipment_embeddings_generated = len(equipment_embed_targets)
        for row, vector in zip(equipment_embed_targets, equipment_vectors):
            row.vector = vector
    else:
        log.info("ib-match: equipment embeddings already exist — skipping regeneration")

    goods_matches, goods_updates = _match_rows(goods_rows, ib_goods)
    equipment_matches, equipment_updates = _match_rows(
        equipment_rows, ib_equipment
    )
    prodclass_matches, prodclass_updates_raw = _match_rows(
        prodclass_rows, ib_prodclass
    )
    prodclass_updates = _normalize_prodclass_updates(prodclass_updates_raw)
    prodclass_clear_ids = [
        match.ai_id for match in prodclass_matches if match.match_ib_id is None
    ]
    log.info(
        "ib-match: prepared %s goods updates and %s equipment updates for client_id=%s",
        len(goods_updates),
        len(equipment_updates),
        client_id,
    )

    goods_vectors_persisted = 0
    equipment_vectors_persisted = 0
    prodclass_updated = 0
    prodclass_inserted = 0
    prodclass_cleared = 0

    async with engine.begin() as conn:
        if goods_embed_targets:
            update_vec_goods_sql = text(
                """
                UPDATE public.ai_site_goods_types
                SET text_vector = CAST(:vec AS vector)
                WHERE id = :id
                """
            )
            for row in goods_embed_targets:
                literal = _vector_to_literal(row.vector)
                if literal is None:
                    log.warning(
                        "ib-match: skipping goods vector update for id=%s — empty vector",
                        row.ai_id,
                    )
                    continue
                await conn.execute(
                    update_vec_goods_sql,
                    {"id": row.ai_id, "vec": literal},
                )
                goods_vectors_persisted += 1
        if equipment_embed_targets:
            update_vec_equipment_sql = text(
                """
                UPDATE public.ai_site_equipment
                SET text_vector = CAST(:vec AS vector)
                WHERE id = :id
                """
            )
            for row in equipment_embed_targets:
                literal = _vector_to_literal(row.vector)
                if literal is None:
                    log.warning(
                        "ib-match: skipping equipment vector update for id=%s — empty vector",
                        row.ai_id,
                    )
                    continue
                await conn.execute(
                    update_vec_equipment_sql,
                    {"id": row.ai_id, "vec": literal},
                )
                equipment_vectors_persisted += 1
        if goods_updates:
            update_goods_sql = text(
                """
                UPDATE public.ai_site_goods_types
                SET goods_type_ID = :match_id,
                    goods_types_score = :score
                WHERE id = :id
                """
            )
            for payload in goods_updates:
                await conn.execute(update_goods_sql, payload)
        if equipment_updates:
            update_equipment_sql = text(
                """
                UPDATE public.ai_site_equipment
                SET equipment_ID = :match_id,
                    equipment_score = :score
                WHERE id = :id
                """
            )
            for payload in equipment_updates:
                await conn.execute(update_equipment_sql, payload)
        if prodclass_clear_ids:
            delete_prodclass_sql = text(
                """
                DELETE FROM public.ai_site_prodclass
                WHERE text_pars_id = :text_pars_id
                """
            )
            for pars_id in prodclass_clear_ids:
                if pars_id not in prodclass_existing_map:
                    continue
                await conn.execute(delete_prodclass_sql, {"text_pars_id": pars_id})
                prodclass_cleared += 1
                prodclass_existing_map.pop(pars_id, None)
        if prodclass_updates:
            update_prodclass_sql = text(
                """
                UPDATE public.ai_site_prodclass
                SET prodclass = :prodclass,
                    prodclass_score = :score
                WHERE id = :row_id
                """
            )
            insert_prodclass_sql = text(
                """
                INSERT INTO public.ai_site_prodclass (text_pars_id, prodclass, prodclass_score)
                VALUES (:text_pars_id, :prodclass, :score)
                RETURNING id
                """
            )
            for payload in prodclass_updates:
                pars_id = int(payload["text_pars_id"])
                row_id = prodclass_existing_map.get(pars_id)
                if row_id is None:
                    insert_result = await conn.execute(insert_prodclass_sql, payload)
                    new_id = insert_result.scalar_one()
                    prodclass_existing_map[pars_id] = new_id
                    prodclass_inserted += 1
                    prodclass_updated += 1
                else:
                    update_payload = dict(payload)
                    update_payload["row_id"] = row_id
                    await conn.execute(update_prodclass_sql, update_payload)
                    prodclass_updated += 1
    log.info(
        "ib-match: database updates applied for client_id=%s (goods_vectors=%s, equipment_vectors=%s, goods_matches=%s, equipment_matches=%s, prodclass_matches=%s, prodclass_cleared=%s)",
        client_id,
        goods_vectors_persisted,
        equipment_vectors_persisted,
        len(goods_updates),
        len(equipment_updates),
        len(prodclass_updates),
        prodclass_cleared,
    )

    goods_updated = len(goods_updates)
    equipment_updated = len(equipment_updates)
    prodclass_assigned = prodclass_updated

    _log_match_details("goods", goods_matches)
    _log_match_details("equipment", equipment_matches)
    _log_match_details("prodclass", prodclass_matches)

    report_text = _build_report(
        client_id=client_id,
        goods_rows=goods_rows,
        equipment_rows=equipment_rows,
        prodclass_rows=prodclass_rows,
        ib_goods=ib_goods,
        ib_equipment=ib_equipment,
        ib_prodclass=ib_prodclass,
        goods_matches=goods_matches,
        equipment_matches=equipment_matches,
        prodclass_matches=prodclass_matches,
        goods_embeddings_generated=goods_embeddings_generated,
        equipment_embeddings_generated=equipment_embeddings_generated,
        goods_updated=goods_updated,
        equipment_updated=equipment_updated,
        prodclass_updated=prodclass_updated,
        prodclass_cleared=prodclass_cleared,
    )

    log.info(
        "ib-match: finished assignment for client_id=%s (goods_processed=%s, equipment_processed=%s, prodclass_processed=%s)",
        client_id,
        len(goods_rows),
        len(equipment_rows),
        len(prodclass_rows),
    )

    duration_ms = int((time.perf_counter() - started_at) * 1000)

    return {
        "client_id": client_id,
        "goods": [_serialize_goods_match(match) for match in goods_matches],
        "equipment": [
            _serialize_equipment_match(match) for match in equipment_matches
        ],
        "prodclass": [
            _serialize_prodclass_match(match) for match in prodclass_matches
        ],
        "summary": {
            "goods_processed": len(goods_rows),
            "goods_total": len(goods_rows),
            "goods_updated": goods_updated,
            "goods_embeddings_generated": goods_embeddings_generated,
            "goods_embedded": goods_embeddings_generated,
            "equipment_processed": len(equipment_rows),
            "equipment_total": len(equipment_rows),
            "equipment_updated": equipment_updated,
            "equipment_embeddings_generated": equipment_embeddings_generated,
            "equipment_embedded": equipment_embeddings_generated,
            "ib_goods_with_vectors": len(ib_goods),
            "catalog_goods_total": len(ib_goods),
            "ib_equipment_with_vectors": len(ib_equipment),
            "catalog_equipment_total": len(ib_equipment),
        "prodclass_processed": len(prodclass_rows),
        "prodclass_total": len(prodclass_rows),
        "prodclass_updated": prodclass_assigned,
        "prodclass_inserted": prodclass_inserted,
        "prodclass_cleared": prodclass_cleared,
        "ib_prodclass_with_vectors": sum(1 for entry in ib_prodclass if entry.vectors),
        "catalog_prodclass_total": len(ib_prodclass),
        },
        "report": report_text,
        "debug_report": report_text,
        "duration_ms": duration_ms,
    }


async def assign_ib_matches_by_inn(*, inn: str, reembed_if_exists: bool) -> dict[str, Any]:
    normalized_inn = (inn or "").strip()
    if not normalized_inn:
        raise IbMatchServiceError("ИНН не может быть пустым", status_code=400)

    engine = get_postgres_engine()
    if engine is None:
        raise IbMatchServiceError("Postgres DSN is not configured", status_code=503)

    async with engine.connect() as conn:
        log.info("ib-match: resolving client_id by INN %s", normalized_inn)
        query = text(
            """
            SELECT id
            FROM public.clients_requests
            WHERE inn = :inn
            ORDER BY COALESCE(ended_at, created_at) DESC NULLS LAST, id DESC
            """
        )
        result = await conn.execute(query, {"inn": normalized_inn})
        candidate_ids = [int(row["id"]) for row in result.mappings()]

    if not candidate_ids:
        log.info("ib-match: clients_requests not found for INN %s", normalized_inn)
        raise IbMatchServiceError("Не найдена запись clients_requests по ИНН", status_code=404)

    log.info(
        "ib-match: found %s client(s) for INN %s → %s",
        len(candidate_ids),
        normalized_inn,
        candidate_ids,
    )
    client_id = candidate_ids[0]
    log.info(
        "ib-match: using clients_requests.id=%s for INN %s (reembed_if_exists=%s)",
        client_id,
        normalized_inn,
        reembed_if_exists,
    )
    result = await assign_ib_matches(
        client_id=client_id,
        reembed_if_exists=reembed_if_exists,
    )
    log.info(
        "ib-match: assignment via INN %s completed (client_id=%s)",
        normalized_inn,
        client_id,
    )
    return result


def _select_for_embedding(rows: Sequence[SourceRow], reembed: bool) -> List[SourceRow]:
    items: List[SourceRow] = []
    for row in rows:
        if reembed or not row.vector:
            if row.text and row.text.strip():
                items.append(row)
    return items


async def _embed_and_update_rows(rows: Sequence[SourceRow]) -> List[List[float]]:
    texts = [row.text.strip() for row in rows]
    vectors: List[List[float]] = []
    for batch in _batched(texts, _MAX_BATCH_SIZE):
        vectors.extend(await _embed_texts(batch))
    return vectors


async def _embed_texts(texts: Sequence[str]) -> List[List[float]]:
    if not texts:
        return []
    base = settings.analyze_base or _DEFAULT_EMBED_BASE
    if not base:
        raise IbMatchServiceError("Embedding service base URL is not configured", status_code=503)
    try:
        await ensure_service_available(base, label="ib-match:embedding")
    except AnalyzeServiceUnavailable as exc:
        log.warning("ib-match: analyze service unavailable: %s", exc)
        raise IbMatchServiceError(str(exc), status_code=503) from exc
    normalized_base = base.rstrip("/")
    timeout = settings.analyze_timeout
    result: List[List[float]] = []
    async with httpx.AsyncClient(base_url=normalized_base, timeout=timeout) as client:
        for index, text in enumerate(texts, start=1):
            try:
                await ensure_analyze_client_health(
                    client,
                    label=f"ib-match:embedding:{index}",
                    target=normalized_base,
                )
            except AnalyzeServiceUnavailable as exc:
                log.warning("ib-match: analyze service unavailable: %s", exc)
                raise IbMatchServiceError(str(exc), status_code=503) from exc
            payload = {"q": text}
            try:
                response = await client.post(_EMBED_ENDPOINT, json=payload)
            except httpx.HTTPError as exc:  # noqa: BLE001
                log.warning("ib-match: embedding request failed: %s", exc)
                raise IbMatchServiceError(
                    f"Не удалось получить эмбеддинг для текста: {exc}", status_code=503
                ) from exc
            if response.status_code >= 400:
                log.warning(
                    "ib-match: embedding service returned %s: %s", response.status_code, response.text
                )
                raise IbMatchServiceError(
                    f"Сервис эмбеддингов вернул код {response.status_code}", status_code=502
                )
            try:
                data = response.json()
            except ValueError as exc:  # noqa: BLE001
                raise IbMatchServiceError(
                    "Сервис эмбеддингов вернул не-JSON ответ", status_code=502
                ) from exc
            embedding = data.get("embedding")
            if not isinstance(embedding, (list, tuple)):
                raise IbMatchServiceError(
                    "Сервис эмбеддингов вернул некорректный ответ", status_code=502
                )
            try:
                vector = [float(x) for x in embedding]
            except (TypeError, ValueError) as exc:
                raise IbMatchServiceError(
                    "Не удалось преобразовать эмбеддинг в список чисел", status_code=502
                ) from exc
            result.append(vector)
    return result


async def _augment_prodclass_catalog(catalog: List[CatalogEntry]) -> int:
    """Добавляет к prodclass запасные векторы на основе текстовых названий."""

    if not catalog:
        return 0

    grouped: Dict[str, List[CatalogEntry]] = {}
    for entry in catalog:
        name = (entry.name or "").strip()
        if not name:
            continue
        grouped.setdefault(name, []).append(entry)

    added = 0
    missing = [name for name in grouped if name not in _catalog_name_embeddings]
    if missing:
        try:
            embedded = await _embed_texts(missing)
        except IbMatchServiceError as exc:
            log.warning("ib-match: не удалось получить эмбеддинги названий prodclass: %s", exc)
            embedded = []
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "ib-match: неожиданная ошибка при эмбеддинге названий prodclass: %s",
                exc,
            )
            embedded = []
        for name, vector in zip(missing, embedded):
            if vector:
                _catalog_name_embeddings[name] = list(vector)

    for name, entries in grouped.items():
        vector = _catalog_name_embeddings.get(name)
        if not vector:
            continue
        for entry in entries:
            if _VECTOR_SOURCE_NAME in entry.vector_sources:
                continue
            entry.add_vector(vector, source=_VECTOR_SOURCE_NAME)
            added += 1
            base_vector = None
            for vec, source in zip(entry.vectors, entry.vector_sources):
                if source == _VECTOR_SOURCE_CATALOG:
                    base_vector = vec
                    break
            if base_vector is not None:
                similarity = cosine_similarity(base_vector, vector)
                if similarity is None or similarity < 0.6:
                    trimmed = entry.name.replace("\n", " ")
                    if len(trimmed) > 80:
                        trimmed = trimmed[:79] + "…"
                    log.warning(
                        "ib-match: prodclass id=%s '%s' каталоговый и name-векторы расходятся (cos=%.3f)",
                        entry.ib_id,
                        trimmed,
                        similarity if similarity is not None else float("nan"),
                    )

    return added


def _normalize_prodclass_updates(
    updates: Sequence[dict[str, Any]]
) -> List[dict[str, Any]]:
    """Подготавливает payload для upsert'ов prodclass."""

    normalized: List[dict[str, Any]] = []
    for payload in updates:
        match_id = payload.get("match_id")
        score = payload.get("score")
        if match_id is None or score is None:
            continue
        normalized.append(
            {
                "text_pars_id": int(payload["id"]),
                "prodclass": int(match_id),
                "score": float(score),
                "prodclass_score": float(score),
            }
        )
    return normalized


def _match_rows(
    rows: Sequence[SourceRow], catalog: Sequence[CatalogEntry]
) -> tuple[List[MatchResult], List[dict[str, Any]]]:
    matches: List[MatchResult] = []
    updates: List[dict[str, Any]] = []
    for row in rows:
        if not row.vector:
            matches.append(
                MatchResult(
                    ai_id=row.ai_id,
                    text=row.text,
                    match_ib_id=None,
                    match_ib_name=None,
                    score=None,
                    note="Нет вектора — пропуск",
                )
            )
            continue
        if not catalog:
            matches.append(
                MatchResult(
                    ai_id=row.ai_id,
                    text=row.text,
                    match_ib_id=None,
                    match_ib_name=None,
                    score=None,
                    note="Нет кандидатов в справочнике",
                )
            )
            continue
        best_id: Optional[int] = None
        best_name: Optional[str] = None
        best_note: str = ""
        best_score = float("-inf")
        has_valid_score = False
        for entry in catalog:
            if not entry.vectors:
                continue
            entry_best = float("-inf")
            entry_source: Optional[str] = None
            entry_has_score = False
            sources = list(entry.vector_sources)
            if len(sources) < len(entry.vectors):
                sources.extend([None] * (len(entry.vectors) - len(sources)))
            for idx, vector in enumerate(entry.vectors):
                source = sources[idx] or _VECTOR_SOURCE_CATALOG
                score = cosine_similarity(row.vector, vector)
                if score is None:
                    continue
                entry_has_score = True
                if score > entry_best:
                    entry_best = score
                    entry_source = source
            if not entry_has_score:
                continue
            has_valid_score = True
            if entry_best > best_score:
                best_id = entry.ib_id
                best_name = entry.name
                best_score = entry_best
                best_note = (
                    "vector_source=" + entry_source
                    if entry_source and entry_source != _VECTOR_SOURCE_CATALOG
                    else ""
                )
        if not has_valid_score:
            matches.append(
                MatchResult(
                    ai_id=row.ai_id,
                    text=row.text,
                    match_ib_id=None,
                    match_ib_name=None,
                    score=None,
                    note="Нет валидных векторов для сравнения",
                )
            )
            continue
        if best_id is None:
            matches.append(
                MatchResult(
                    ai_id=row.ai_id,
                    text=row.text,
                    match_ib_id=None,
                    match_ib_name=None,
                    score=None,
                    note="Кандидаты не найдены",
                )
            )
            continue
        clamped_score = min(max(best_score, 0.0), 1.0)
        rounded_score = round(clamped_score, 4)
        rounded_score_db = rounded_score
        matches.append(
            MatchResult(
                ai_id=row.ai_id,
                text=row.text,
                match_ib_id=best_id,
                match_ib_name=best_name,
                score=rounded_score,
                note=best_note,
            )
        )
        updates.append(
            {
                "id": row.ai_id,
                "match_id": best_id,
                "score": rounded_score_db,
            }
        )
    return matches, updates


def _log_match_details(entity: str, matches: Sequence[MatchResult]) -> None:
    for match in matches:
        if match.match_ib_id is not None and match.score is not None:
            suffix = f" ({match.note})" if match.note else ""
            log.info(
                "ib-match: %s id=%s '%s' → ib_id=%s '%s' score=%.4f%s",
                entity,
                match.ai_id,
                _clip(match.text),
                match.match_ib_id,
                _clip(match.match_ib_name or ""),
                match.score,
                suffix,
            )
        else:
            reason = match.note or "нет соответствия"
            log.info(
                "ib-match: %s id=%s '%s' → — (%s)",
                entity,
                match.ai_id,
                _clip(match.text),
                reason,
            )


def _parse_pgvector(value: Any) -> Optional[List[float]]:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        try:
            return [float(x) for x in value]
        except (TypeError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if not text:
        return None
    try:
        return [float(part) for part in text.split(",") if part.strip()]
    except ValueError:
        return None


def _clip(value: Optional[str], *, limit: int = 80) -> str:
    if not value:
        return ""
    value = value.replace("\n", " ")
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _vector_to_literal(vector: Optional[Sequence[float]]) -> Optional[str]:
    if vector is None:
        return None
    return "[" + ",".join(f"{float(x):.7f}" for x in vector) + "]"


def _batched(seq: Sequence[str], size: int) -> Iterable[List[str]]:
    chunk: List[str] = []
    for item in seq:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _build_report(
    *,
    client_id: int,
    goods_rows: Sequence[SourceRow],
    equipment_rows: Sequence[SourceRow],
    prodclass_rows: Sequence[SourceRow],
    ib_goods: Sequence[CatalogEntry],
    ib_equipment: Sequence[CatalogEntry],
    ib_prodclass: Sequence[CatalogEntry],
    goods_matches: Sequence[MatchResult],
    equipment_matches: Sequence[MatchResult],
    prodclass_matches: Sequence[MatchResult],
    goods_embeddings_generated: int,
    equipment_embeddings_generated: int,
    goods_updated: int,
    equipment_updated: int,
    prodclass_updated: int,
    prodclass_cleared: int,
) -> str:
    lines: List[str] = []
    lines.append(
        f"[INFO] Найдено записей по CLIENT_ID={client_id}: goods_types={len(goods_rows)}, equipment={len(equipment_rows)}, pars_site={len(prodclass_rows)}"
    )
    if goods_rows:
        lines.append("")
        lines.append("— Связанные ai_site_goods_types.goods_type:")
        for row in goods_rows:
            lines.append(f"   [goods_id={row.ai_id}] {row.text}")
    if equipment_rows:
        lines.append("")
        lines.append("— Связанные ai_site_equipment.equipment:")
        for row in equipment_rows:
            lines.append(f"   [equip_id={row.ai_id}] {row.text}")
    if prodclass_rows:
        lines.append("")
        lines.append("— Связанные pars_site.text_vector:")
        for row in prodclass_rows:
            lines.append(f"   [pars_id={row.ai_id}] {row.text}")
    lines.append("")
    lines.append(
        f"[INFO] В справочнике ib_goods_types: {len(ib_goods)} позиций с валидными векторами."
    )
    lines.append(
        f"[INFO] В справочнике ib_equipment: {len(ib_equipment)} позиций с валидными векторами."
    )
    lines.append(
        f"[INFO] В справочнике ib_prodclass: {sum(1 for entry in ib_prodclass if entry.vectors)} позиций с валидными векторами."
    )
    lines.append("")
    lines.append(
        _format_embedding_line(
            "goods_types", goods_embeddings_generated, len(goods_rows)
        )
    )
    lines.append(
        _format_embedding_line(
            "equipment", equipment_embeddings_generated, len(equipment_rows)
        )
    )

    lines.append("")
    lines.append("=" * _BORDER_WIDTH)
    lines.append("ИТОГОВОЕ СООТВЕТСТВИЕ: ТИПЫ ПРОДУКЦИИ (ai_site_goods_types → ib_goods_types)")
    if goods_matches:
        lines.extend(_format_table(
            headers=[
                "ai_goods_id",
                "ai_goods_type",
                "match_ib_id",
                "match_ib_name",
                "score",
                "note",
            ],
            rows=[
                [
                    match.ai_id,
                    match.text.replace("\n", " "),
                    match.match_ib_id if match.match_ib_id is not None else "—",
                    match.match_ib_name or "—",
                    f"{match.score:.4f}" if match.score is not None else "—",
                    match.note or "",
                ]
                for match in goods_matches
            ],
        ))
    else:
        lines.append("[ПУСТО] Нет записей для отображения.")

    lines.append("")
    lines.append("=" * _BORDER_WIDTH)
    lines.append("ИТОГОВОЕ СООТВЕТСТВИЕ: ОБОРУДОВАНИЕ (ai_site_equipment → ib_equipment)")
    if equipment_matches:
        lines.extend(_format_table(
            headers=[
                "ai_equip_id",
                "ai_equipment",
                "match_ib_id",
                "match_ib_name",
                "score",
                "note",
            ],
            rows=[
                [
                    match.ai_id,
                    match.text.replace("\n", " "),
                    match.match_ib_id if match.match_ib_id is not None else "—",
                    match.match_ib_name or "—",
                    f"{match.score:.4f}" if match.score is not None else "—",
                    match.note or "",
                ]
                for match in equipment_matches
            ],
        ))
    else:
        lines.append("[ПУСТО] Нет записей для отображения.")

    lines.append("")
    lines.append("=" * _BORDER_WIDTH)
    lines.append("ИТОГОВОЕ СООТВЕТСТВИЕ: PRODCLASS (pars_site → ib_prodclass)")
    if prodclass_matches:
        lines.extend(_format_table(
            headers=[
                "pars_site_id",
                "label",
                "match_ib_id",
                "match_ib_name",
                "score",
                "note",
            ],
            rows=[
                [
                    match.ai_id,
                    match.text.replace("\n", " "),
                    match.match_ib_id if match.match_ib_id is not None else "—",
                    match.match_ib_name or "—",
                    f"{match.score:.4f}" if match.score is not None else "—",
                    match.note or "",
                ]
                for match in prodclass_matches
            ],
        ))
    else:
        lines.append("[ПУСТО] Нет записей для отображения.")

    lines.append("")
    lines.append("=" * _BORDER_WIDTH)
    lines.append("СВОДКА:")
    lines.append(f"- CLIENT_ID: {client_id}")
    lines.append(f"- Обработано goods_types: {len(goods_rows)}, обновлено: {goods_updated}")
    lines.append(f"- Обработано equipment:   {len(equipment_rows)}, обновлено: {equipment_updated}")
    lines.append(
        f"- Обработано pars_site:   {len(prodclass_rows)}, назначено/обновлено: {prodclass_updated}, очищено: {prodclass_cleared}"
    )
    lines.append(f"- В ib_goods_types: {len(ib_goods)} позиций с векторами")
    lines.append(f"- В ib_equipment:   {len(ib_equipment)} позиций с векторами")
    lines.append(
        f"- В ib_prodclass:   {sum(1 for entry in ib_prodclass if entry.vectors)} позиций с векторами"
    )
    lines.append("=" * _BORDER_WIDTH)

    if goods_matches:
        lines.append("")
        lines.append("[ПОДБОР GOODS] Наиболее релевантные соответствия:")
        for match in goods_matches:
            if match.match_ib_id is not None and match.score is not None:
                lines.append(
                    f"  ai_site_goods_types(id={match.ai_id}): '{match.text}' → ib_goods_types '{match.match_ib_name}' (score={match.score:.4f})"
                )
            else:
                reason = match.note or "нет соответствия"
                lines.append(
                    f"  ai_site_goods_types(id={match.ai_id}): '{match.text}' → — ({reason})"
                )
    if equipment_matches:
        lines.append("")
        lines.append("[ПОДБОР EQUIPMENT] Наиболее релевантные соответствия:")
        for match in equipment_matches:
            if match.match_ib_id is not None and match.score is not None:
                lines.append(
                    f"  ai_site_equipment(id={match.ai_id}): '{match.text}' → ib_equipment '{match.match_ib_name}' (score={match.score:.4f})"
                )
            else:
                reason = match.note or "нет соответствия"
                lines.append(
                    f"  ai_site_equipment(id={match.ai_id}): '{match.text}' → — ({reason})"
                )
    if prodclass_matches:
        lines.append("")
        lines.append("[ПОДБОР PRODCLASS] Наиболее релевантные соответствия:")
        for match in prodclass_matches:
            if match.match_ib_id is not None and match.score is not None:
                lines.append(
                    f"  pars_site(id={match.ai_id}): '{match.text}' → ib_prodclass '{match.match_ib_name}' (score={match.score:.4f})"
                )
            else:
                reason = match.note or "нет соответствия"
                lines.append(
                    f"  pars_site(id={match.ai_id}): '{match.text}' → — ({reason})"
                )

    return "\n".join(lines)


def _format_embedding_line(kind: str, generated: int, total: int) -> str:
    base = f"[EMBED] Генерируем эмбеддинги для {kind}: {generated} из {total}"
    if generated:
        return base
    return base + " (эмбеддинги не требуются)"


def _format_table(*, headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> List[str]:
    if not rows:
        return ["[ПУСТО] Нет записей для отображения."]
    widths = [len(str(header)) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(str(cell)))

    def border(char: str = "-") -> str:
        return "+" + "+".join(char * (w + 2) for w in widths) + "+"

    def fmt(row: Sequence[Any]) -> str:
        return "|" + "|".join(
            f" {str(cell):<{widths[idx]}} " for idx, cell in enumerate(row)
        ) + "|"

    lines = [border("-")]
    lines.append(fmt(headers))
    lines.append(border("-"))
    for row in rows:
        lines.append(fmt(row))
    lines.append(border("-"))
    return lines


__all__ = ["assign_ib_matches", "IbMatchServiceError"]
