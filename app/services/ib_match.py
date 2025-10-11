from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence

import httpx
from sqlalchemy import text

from app.config import settings
from app.db.postgres import get_postgres_engine

log = logging.getLogger("services.ib_match")

_DEFAULT_EMBED_BASE = "http://37.221.125.221:8123"
_EMBED_ENDPOINT = "/ai-search"
_MAX_BATCH_SIZE = 64
_BORDER_WIDTH = 88


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
    vector: List[float]


@dataclass
class MatchResult:
    ai_id: int
    text: str
    match_ib_id: Optional[int]
    match_ib_name: Optional[str]
    score: Optional[float]
    note: str = ""


async def assign_ib_matches(*, client_id: int, reembed_if_exists: bool) -> dict[str, Any]:
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
            ib_goods.append(
                CatalogEntry(
                    ib_id=int(row["id"]),
                    name=str(row[settings.IB_GOODS_TYPES_NAME_COLUMN] or ""),
                    vector=vector,
                )
            )

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
            ib_equipment.append(
                CatalogEntry(
                    ib_id=int(row["id"]),
                    name=str(row[settings.IB_EQUIPMENT_NAME_COLUMN] or ""),
                    vector=vector,
                )
            )

    goods_embed_targets = _select_for_embedding(goods_rows, reembed_if_exists)
    equipment_embed_targets = _select_for_embedding(equipment_rows, reembed_if_exists)

    goods_embeddings_generated = 0
    equipment_embeddings_generated = 0

    if goods_embed_targets:
        log.info("ib-match: generating %s goods embeddings", len(goods_embed_targets))
        goods_vectors = await _embed_and_update_rows(goods_embed_targets)
        goods_embeddings_generated = len(goods_embed_targets)
        for row, vector in zip(goods_embed_targets, goods_vectors):
            row.vector = vector
    if equipment_embed_targets:
        log.info("ib-match: generating %s equipment embeddings", len(equipment_embed_targets))
        equipment_vectors = await _embed_and_update_rows(equipment_embed_targets)
        equipment_embeddings_generated = len(equipment_embed_targets)
        for row, vector in zip(equipment_embed_targets, equipment_vectors):
            row.vector = vector

    goods_matches, goods_updates = _match_rows(goods_rows, ib_goods)
    equipment_matches, equipment_updates = _match_rows(equipment_rows, ib_equipment)

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
                    continue
                await conn.execute(
                    update_vec_goods_sql,
                    {"id": row.ai_id, "vec": literal},
                )
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
                    continue
                await conn.execute(
                    update_vec_equipment_sql,
                    {"id": row.ai_id, "vec": literal},
                )
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

    goods_updated = len(goods_updates)
    equipment_updated = len(equipment_updates)

    report_text = _build_report(
        client_id=client_id,
        goods_rows=goods_rows,
        equipment_rows=equipment_rows,
        ib_goods=ib_goods,
        ib_equipment=ib_equipment,
        goods_matches=goods_matches,
        equipment_matches=equipment_matches,
        goods_embeddings_generated=goods_embeddings_generated,
        equipment_embeddings_generated=equipment_embeddings_generated,
        goods_updated=goods_updated,
        equipment_updated=equipment_updated,
    )

    return {
        "client_id": client_id,
        "goods": [match.__dict__ for match in goods_matches],
        "equipment": [match.__dict__ for match in equipment_matches],
        "summary": {
            "goods_processed": len(goods_rows),
            "goods_updated": goods_updated,
            "goods_embeddings_generated": goods_embeddings_generated,
            "equipment_processed": len(equipment_rows),
            "equipment_updated": equipment_updated,
            "equipment_embeddings_generated": equipment_embeddings_generated,
            "ib_goods_with_vectors": len(ib_goods),
            "ib_equipment_with_vectors": len(ib_equipment),
        },
        "report": report_text,
    }


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
    url = base.rstrip("/") + _EMBED_ENDPOINT
    timeout = settings.analyze_timeout
    result: List[List[float]] = []
    async with httpx.AsyncClient(timeout=timeout) as client:
        for text in texts:
            payload = {"q": text}
            try:
                response = await client.post(url, json=payload)
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
        best_score = -1.0
        for entry in catalog:
            score = _cosine_similarity(row.vector, entry.vector)
            if score > best_score:
                best_id = entry.ib_id
                best_name = entry.name
                best_score = score
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
        rounded_score = round(best_score, 4)
        matches.append(
            MatchResult(
                ai_id=row.ai_id,
                text=row.text,
                match_ib_id=best_id,
                match_ib_name=best_name,
                score=rounded_score,
                note="",
            )
        )
        updates.append(
            {
                "id": row.ai_id,
                "match_id": best_id,
                "score": round(best_score, 2),
            }
        )
    return matches, updates


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b:
        return 0.0
    if len(a) != len(b):
        length = min(len(a), len(b))
        a = a[:length]
        b = b[:length]
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    denom = norm_a * norm_b
    if denom == 0:
        return 0.0
    value = dot / denom
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return value


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
    ib_goods: Sequence[CatalogEntry],
    ib_equipment: Sequence[CatalogEntry],
    goods_matches: Sequence[MatchResult],
    equipment_matches: Sequence[MatchResult],
    goods_embeddings_generated: int,
    equipment_embeddings_generated: int,
    goods_updated: int,
    equipment_updated: int,
) -> str:
    lines: List[str] = []
    lines.append(
        f"[INFO] Найдено записей по CLIENT_ID={client_id}: goods_types={len(goods_rows)}, equipment={len(equipment_rows)}"
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
    lines.append("")
    lines.append(
        f"[INFO] В справочнике ib_goods_types: {len(ib_goods)} позиций с валидными векторами."
    )
    lines.append(
        f"[INFO] В справочнике ib_equipment: {len(ib_equipment)} позиций с валидными векторами."
    )
    lines.append("")
    if goods_embeddings_generated:
        lines.append(f"[EMBED] Генерируем эмбеддинги для goods_types: {goods_embeddings_generated}")
    else:
        lines.append("[EMBED] Эмбеддинги для goods_types не требуются (все присутствуют).")
    if equipment_embeddings_generated:
        lines.append(f"[EMBED] Генерируем эмбеддинги для equipment: {equipment_embeddings_generated}")
    else:
        lines.append("[EMBED] Эмбеддинги для equipment не требуются (все присутствуют).")

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
                    match.text,
                    match.match_ib_id if match.match_ib_id is not None else "",
                    match.match_ib_name or "",
                    f"{match.score:.4f}" if match.score is not None else "",
                    match.note,
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
                    match.text,
                    match.match_ib_id if match.match_ib_id is not None else "",
                    match.match_ib_name or "",
                    f"{match.score:.4f}" if match.score is not None else "",
                    match.note,
                ]
                for match in equipment_matches
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
    lines.append(f"- В ib_goods_types: {len(ib_goods)} позиций с векторами")
    lines.append(f"- В ib_equipment:   {len(ib_equipment)} позиций с векторами")
    lines.append("=" * _BORDER_WIDTH)

    if goods_matches:
        lines.append("")
        lines.append("[ПОДБОР GOODS] Наиболее релевантные соответствия:")
        for match in goods_matches:
            if match.match_ib_id is not None and match.score is not None:
                lines.append(
                    f"  ai_site_goods_types(id={match.ai_id}): '{match.text}' → ib_goods_types '{match.match_ib_name}' (score={match.score:.4f})"
                )
    if equipment_matches:
        lines.append("")
        lines.append("[ПОДБОР EQUIPMENT] Наиболее релевантные соответствия:")
        for match in equipment_matches:
            if match.match_ib_id is not None and match.score is not None:
                lines.append(
                    f"  ai_site_equipment(id={match.ai_id}): '{match.text}' → ib_equipment '{match.match_ib_name}' (score={match.score:.4f})"
                )

    return "\n".join(lines)


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
