"""Сервис расчёта наиболее подходящего оборудования."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, Field
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncConnection

log = logging.getLogger("services.equipment_selection")


class EquipmentSelectionNotFound(Exception):
    """Выбрана несуществующая запись клиента."""


_FOUR_DECIMALS = Decimal("0.0001")


def _to_decimal(value: object, default: Decimal = Decimal(0)) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _quantize(score: Decimal) -> Decimal:
    return score.quantize(_FOUR_DECIMALS)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _row_mapping(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    if hasattr(row, "_mapping"):
        return dict(row._mapping)  # type: ignore[attr-defined]
    if hasattr(row, "__dict__"):
        return dict(vars(row))
    return dict(row)


@dataclass(slots=True)
class EquipmentScore:
    id: int
    name: Optional[str]
    score: Decimal
    source: str


@dataclass(slots=True)
class TableData:
    step: str
    table_id: str
    title: str
    columns: List[str]
    rows: List[List[Any]]


@dataclass(slots=True)
class EquipmentStepReport:
    client_rows: Sequence[Dict[str, Any]] = field(default_factory=list)
    goods_types: Sequence[Dict[str, Any]] = field(default_factory=list)
    site_equipment: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_rows: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_agg: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_paths: Sequence[Dict[str, Any]] = field(default_factory=list)
    equipment_1way: Sequence[EquipmentScore] = field(default_factory=list)
    equipment_1way_details: Sequence[Dict[str, Any]] = field(default_factory=list)
    equipment_2way: Sequence[EquipmentScore] = field(default_factory=list)
    equipment_2way_goods: Sequence[Dict[str, Any]] = field(default_factory=list)
    equipment_2way_details: Sequence[Dict[str, Any]] = field(default_factory=list)
    equipment_3way: Sequence[EquipmentScore] = field(default_factory=list)
    equipment_3way_details: Sequence[Dict[str, Any]] = field(default_factory=list)
    equipment_all: Sequence[EquipmentScore] = field(default_factory=list)
    equipment_all_sources: Sequence[Dict[str, Any]] = field(default_factory=list)
    tables: List[TableData] = field(default_factory=list)
    log: List[str] = field(default_factory=list)


class TableDataModel(BaseModel):
    step: str = Field(description="Номер шага (например, '2.b').")
    table_id: str = Field(description="Внутренний идентификатор таблицы.")
    title: str = Field(description="Человекочитаемый заголовок таблицы.")
    columns: List[str] = Field(description="Названия колонок в порядке отображения.")
    rows: List[List[Any]] = Field(description="Строки таблицы (значения по колонкам).")
    row_count: int = Field(description="Количество строк в таблице.")


class EquipmentSelectionResult(BaseModel):
    client_request_id: int = Field(description="Идентификатор clients_requests.id.")
    tables: List[TableDataModel] = Field(description="Список таблиц, отсортированных по шагам расчёта.")
    log: List[str] = Field(description="Подробный журнал выполнения расчёта.")


async def compute_equipment_selection(
    conn: AsyncConnection, client_request_id: int
) -> EquipmentSelectionResult:
    log.info(
        "equipment-selection: запуск расчёта для clients_requests.id=%s",
        client_request_id,
    )
    report = await build_equipment_tables(conn, client_request_id)
    tables = [
        TableDataModel(
            step=table.step,
            table_id=table.table_id,
            title=table.title,
            columns=table.columns,
            rows=[[ _jsonable(value) for value in row ] for row in table.rows],
            row_count=len(table.rows),
        )
        for table in report.tables
    ]
    return EquipmentSelectionResult(
        client_request_id=client_request_id,
        tables=tables,
        log=list(report.log),
    )


async def resolve_client_request_id(conn: AsyncConnection, inn: str) -> Optional[int]:
    stmt = text(
        """
        SELECT id
        FROM public.clients_requests
        WHERE inn = :inn
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = (await conn.execute(stmt, {"inn": inn.strip()})).first()
    if row is None:
        log.info(
            "equipment-selection: для ИНН %s не найдены записи clients_requests",
            inn,
        )
        return None
    resolved = int(row[0])
    log.info(
        "equipment-selection: ИНН %s сопоставлен с clients_requests.id=%s",
        inn,
        resolved,
    )
    return resolved


async def build_equipment_tables(
    conn: AsyncConnection, client_request_id: int
) -> EquipmentStepReport:
    report = EquipmentStepReport()
    report.log.append(
        f"Старт расчёта оборудования для clients_requests.id={client_request_id}."
    )

    log.debug(
        "equipment-selection: начинаем сбор данных для clients_requests.id=%s",
        client_request_id,
    )

    client_rows = await _load_client(conn, client_request_id)
    report.client_rows = client_rows
    _add_table(
        report,
        step="0",
        table_id="client",
        title=f"Клиент (clients_requests.id={client_request_id})",
        rows=client_rows,
        columns=[
            "id",
            "company_name",
            "inn",
            "domain_1",
            "started_at",
            "ended_at",
        ],
    )
    report.log.append(
        f"Шаг 0: загружена карточка клиента (строк: {len(client_rows)})."
    )

    goods_types = await _load_goods_types(conn, client_request_id)
    report.goods_types = goods_types
    _add_table(
        report,
        step="1.a",
        table_id="goods_types",
        title="1.a) Типы продукции (ai_site_goods_types)",
        rows=goods_types,
        columns=[
            "id",
            "goods_type",
            "goods_type_id",
            "goods_types_score",
            "text_par_id",
            "url",
            "created_at",
        ],
    )
    report.log.append(
        f"Шаг 1.a: найдено {len(goods_types)} записей ai_site_goods_types."
    )

    site_equipment = await _load_site_equipment(conn, client_request_id)
    report.site_equipment = site_equipment
    _add_table(
        report,
        step="1.b",
        table_id="site_equipment",
        title="1.b) Оборудование с сайта (ai_site_equipment)",
        rows=site_equipment,
        columns=[
            "id",
            "equipment",
            "equipment_id",
            "equipment_score",
            "text_pars_id",
            "url",
            "created_at",
        ],
    )
    report.log.append(
        f"Шаг 1.b: найдено {len(site_equipment)} элементов оборудования с сайта."
    )

    prodclass_rows = await _load_prodclass_rows(conn, client_request_id)
    report.prodclass_rows = prodclass_rows
    _add_table(
        report,
        step="2.a",
        table_id="prodclass_rows",
        title="2.a) Записи ai_site_prodclass",
        rows=prodclass_rows,
        columns=[
            "ai_row_id",
            "prodclass_id",
            "prodclass_name",
            "prodclass_score",
            "text_pars_id",
            "url",
            "created_at",
        ],
    )
    report.log.append(
        f"Шаг 2.a: загружено {len(prodclass_rows)} записей ai_site_prodclass."
    )

    prodclass_agg = _aggregate_prodclass(prodclass_rows)
    report.prodclass_agg = prodclass_agg
    _add_table(
        report,
        step="2.b",
        table_id="prodclass_agg",
        title="2.b) Средние prodclass_score (SCORE_1)",
        rows=prodclass_agg,
        columns=["prodclass_id", "prodclass_name", "SCORE_1", "votes"],
    )
    report.log.append(
        f"Шаг 2.b: агрегировано {len(prodclass_agg)} prodclass со средним SCORE_1."
    )

    (
        equipment_1way,
        path_log,
        equipment_1way_details,
    ) = await _compute_equipment_1way(conn, prodclass_agg, report.log)
    report.equipment_1way = equipment_1way
    report.prodclass_paths = path_log
    report.equipment_1way_details = equipment_1way_details

    _add_table(
        report,
        step="2.c",
        table_id="prodclass_paths",
        title="2.c) Пути расчёта по prodclass",
        rows=path_log,
        columns=[
            "prodclass_id",
            "prodclass_name",
            "path",
            "workshops",
            "equipment",
        ],
    )
    _add_table(
        report,
        step="2.c",
        table_id="equipment_1way_details",
        title="2.c) SCORE_E1 по оборудованию",
        rows=equipment_1way_details,
        columns=[
            "id",
            "equipment_name",
            "workshop_id",
            "equipment_score",
            "equipment_score_real",
            "equipment_score_max",
            "SCORE_1",
            "factor",
            "SCORE_E1",
            "path",
        ],
    )
    _add_table(
        report,
        step="2.d",
        table_id="equipment_1way",
        title="2.d) EQUIPMENT_1way (ID, equipment_name, SCORE)",
        rows=equipment_1way,
        columns=["id", "name", "score", "source"],
    )

    (
        equipment_2way,
        goods_type_scores,
        equipment_2way_details,
    ) = await _compute_equipment_2way(conn, goods_types, report.log)
    report.equipment_2way = equipment_2way
    report.equipment_2way_goods = goods_type_scores
    report.equipment_2way_details = equipment_2way_details

    _add_table(
        report,
        step="3.a",
        table_id="equipment_2way_goods",
        title="3.a) Goods_type и CRORE_2",
        rows=goods_type_scores,
        columns=["goods_type_id", "CRORE_2"],
    )
    _add_table(
        report,
        step="3.c",
        table_id="equipment_2way_details",
        title="3.c) SCORE_E2 = CRORE_2 × CRORE_3",
        rows=equipment_2way_details,
        columns=[
            "id",
            "equipment_name",
            "goods_type_id",
            "CRORE_2",
            "CRORE_3",
            "SCORE_E2",
        ],
    )
    _add_table(
        report,
        step="3.d",
        table_id="equipment_2way",
        title="3.d) EQUIPMENT_2way (ID, equipment_name, SCORE)",
        rows=equipment_2way,
        columns=["id", "name", "score", "source"],
    )

    equipment_3way, equipment_3way_details = await _compute_equipment_3way(
        conn, site_equipment, report.log
    )
    report.equipment_3way = equipment_3way
    report.equipment_3way_details = equipment_3way_details

    _add_table(
        report,
        step="4.a",
        table_id="equipment_3way_details",
        title="4.a) Источник SCORE_E3",
        rows=equipment_3way_details,
        columns=["equipment_id", "equipment_score", "SCORE_E3"],
    )
    _add_table(
        report,
        step="4.b",
        table_id="equipment_3way",
        title="4.b) EQUIPMENT_3way (ID, equipment_name, SCORE)",
        rows=equipment_3way,
        columns=["id", "name", "score", "source"],
    )

    equipment_all, equipment_all_sources = _merge_equipment_tables(
        equipment_1way, equipment_2way, equipment_3way, report.log
    )
    report.equipment_all = equipment_all
    report.equipment_all_sources = equipment_all_sources

    _add_table(
        report,
        step="5.a",
        table_id="equipment_all_sources",
        title="5.a) Объединённый список до очистки",
        rows=equipment_all_sources,
        columns=["id", "equipment_name", "score", "source", "priority"],
    )
    _add_table(
        report,
        step="5.b",
        table_id="equipment_all",
        title="5.b) EQUIPMENT_ALL (после дедупликации)",
        rows=equipment_all,
        columns=["id", "name", "score", "source"],
    )

    await _sync_equipment_table(conn, "EQUIPMENT_1way", equipment_1way, report.log)
    await _sync_equipment_table(conn, "EQUIPMENT_2way", equipment_2way, report.log)
    await _sync_equipment_table(conn, "EQUIPMENT_3way", equipment_3way, report.log)
    await _sync_equipment_table(conn, "EQUIPMENT_ALL", equipment_all, report.log)

    report.log.append(
        "Расчёт оборудования завершён. Обновлены таблицы EQUIPMENT_* в базе данных."
    )
    log.info(
        "equipment-selection: расчёт завершён для clients_requests.id=%s",
        client_request_id,
    )
    return report


async def _load_client(
    conn: AsyncConnection, client_request_id: int
) -> List[Dict[str, Any]]:
    stmt = text(
        """
        SELECT id, company_name, inn, domain_1, started_at, ended_at
        FROM public.clients_requests
        WHERE id = :cid
        """
    )
    result = await conn.execute(stmt, {"cid": client_request_id})
    rows = list(result.mappings())
    if not rows:
        raise EquipmentSelectionNotFound(
            f"clients_requests.id={client_request_id} не найден"
        )
    return rows


async def _load_goods_types(
    conn: AsyncConnection, client_request_id: int
) -> List[Dict[str, Any]]:
    stmt = text(
        """
        SELECT
            gst.id,
            gst.goods_type,
            gst.goods_type_id,
            gst.goods_types_score,
            pst.id AS text_par_id,
            pst.url,
            gst.created_at
        FROM ai_site_goods_types AS gst
        JOIN pars_site AS pst ON pst.id = gst.text_par_id
        WHERE pst.company_id = :cid
        ORDER BY gst.created_at, gst.id
        """
    )
    result = await conn.execute(stmt, {"cid": client_request_id})
    return list(result.mappings())


async def _load_site_equipment(
    conn: AsyncConnection, client_request_id: int
) -> List[Dict[str, Any]]:
    stmt = text(
        """
        SELECT
            eq.id,
            eq.equipment,
            eq.equipment_id,
            eq.equipment_score,
            pst.id AS text_pars_id,
            pst.url,
            eq.created_at
        FROM ai_site_equipment AS eq
        JOIN pars_site AS pst ON pst.id = eq.text_pars_id
        WHERE pst.company_id = :cid
        ORDER BY eq.created_at, eq.id
        """
    )
    result = await conn.execute(stmt, {"cid": client_request_id})
    return list(result.mappings())


async def _load_prodclass_rows(
    conn: AsyncConnection, client_request_id: int
) -> List[Dict[str, Any]]:
    stmt = text(
        """
        SELECT
            ap.id AS ai_row_id,
            ap.prodclass AS prodclass_id,
            ip.prodclass AS prodclass_name,
            ap.prodclass_score,
            ap.text_pars_id,
            pst.url,
            ap.created_at
        FROM ai_site_prodclass AS ap
        JOIN pars_site AS pst ON pst.id = ap.text_pars_id
        JOIN ib_prodclass AS ip ON ip.id = ap.prodclass
        WHERE pst.company_id = :cid
        ORDER BY ap.created_at, ap.id
        """
    )
    result = await conn.execute(stmt, {"cid": client_request_id})
    return list(result.mappings())


def _aggregate_prodclass(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    aggregated: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        prodclass_id = int(row["prodclass_id"])
        score = row.get("prodclass_score")
        if score is None:
            continue
        entry = aggregated.setdefault(
            prodclass_id,
            {
                "prodclass_id": prodclass_id,
                "prodclass_name": row.get("prodclass_name"),
                "total": Decimal(0),
                "votes": 0,
            },
        )
        entry["total"] += _to_decimal(score)
        entry["votes"] += 1

    result: List[Dict[str, Any]] = []
    for prodclass_id, entry in aggregated.items():
        votes = entry["votes"] or 1
        score = _quantize(entry["total"] / votes)
        result.append(
            {
                "prodclass_id": prodclass_id,
                "prodclass_name": entry.get("prodclass_name"),
                "SCORE_1": score,
                "votes": entry["votes"],
            }
        )

    result.sort(key=lambda item: (item["SCORE_1"], item["votes"]), reverse=True)
    return result


async def _compute_equipment_1way(
    conn: AsyncConnection,
    prodclass_agg: Sequence[Dict[str, Any]],
    log_messages: List[str],
) -> Tuple[List[EquipmentScore], List[Dict[str, Any]], List[Dict[str, Any]]]:
    equipment_rows: Dict[int, EquipmentScore] = {}
    path_log: List[Dict[str, Any]] = []
    details: List[Dict[str, Any]] = []
    direct_updates = 0
    fallback_updates = 0

    for row in prodclass_agg:
        prodclass_id = int(row["prodclass_id"])
        score_1 = _to_decimal(row["SCORE_1"])
        pc_name = row.get("prodclass_name")

        workshops = await _fetch_workshops(conn, [prodclass_id])
        if workshops:
            equipments = await _fetch_equipment_by_workshops(
                conn, [int(w["id"]) for w in workshops]
            )
            updated = _apply_equipment_scores(
                equipments,
                score_1,
                Decimal("1"),
                equipment_rows,
                details,
                path="direct",
            )
            path_log.append(
                {
                    "prodclass_id": prodclass_id,
                    "prodclass_name": pc_name,
                    "path": "direct",
                    "workshops": len(workshops),
                    "equipment": updated,
                }
            )
            direct_updates += updated
            continue

        industry_id = await _fetch_industry(conn, prodclass_id)
        if industry_id is None:
            path_log.append(
                {
                    "prodclass_id": prodclass_id,
                    "prodclass_name": pc_name,
                    "path": "fallback_missing_industry",
                    "workshops": 0,
                    "equipment": 0,
                }
            )
            continue

        related_prodclasses = await _fetch_prodclass_ids_by_industry(conn, industry_id)
        workshops_fb = await _fetch_workshops(conn, related_prodclasses)
        if not workshops_fb:
            path_log.append(
                {
                    "prodclass_id": prodclass_id,
                    "prodclass_name": pc_name,
                    "path": "fallback_no_workshops",
                    "workshops": 0,
                    "equipment": 0,
                }
            )
            continue

        equipments_fb = await _fetch_equipment_by_workshops(
            conn, [int(w["id"]) for w in workshops_fb]
        )
        updated = _apply_equipment_scores(
            equipments_fb,
            score_1,
            Decimal("0.75"),
            equipment_rows,
            details,
            path="fallback",
        )
        path_log.append(
            {
                "prodclass_id": prodclass_id,
                "prodclass_name": pc_name,
                "path": "fallback",
                "workshops": len(workshops_fb),
                "equipment": updated,
            }
        )
        fallback_updates += updated

    rows_sorted = sorted(equipment_rows.values(), key=lambda item: item.id)
    log_messages.append(
        "Шаг 2: SCORE_E1 рассчитан для "
        f"{len(rows_sorted)} позиций (direct={direct_updates}, fallback={fallback_updates})."
    )
    return rows_sorted, path_log, details


async def _fetch_workshops(
    conn: AsyncConnection, prodclass_ids: Sequence[int]
) -> List[Dict[str, Any]]:
    if not prodclass_ids:
        return []
    stmt = (
        text(
            """
            SELECT id, workshop_name, workshop_score, prodclass_id, company_id
            FROM ib_workshops
            WHERE prodclass_id IN :pc_list
            ORDER BY id
            """
        ).bindparams(bindparam("pc_list", expanding=True))
    )
    result = await conn.execute(stmt, {"pc_list": list(prodclass_ids)})
    return list(result.mappings())


async def _fetch_equipment_by_workshops(
    conn: AsyncConnection, workshop_ids: Sequence[int]
) -> List[Dict[str, Any]]:
    if not workshop_ids:
        return []
    stmt = (
        text(
            """
            SELECT
                e.id,
                e.equipment_name,
                e.workshop_id,
                e.equipment_score,
                e.equipment_score_real,
                GREATEST(e.equipment_score, COALESCE(e.equipment_score_real, 0))
                    AS equipment_score_max
            FROM ib_equipment AS e
            WHERE e.workshop_id IN :ws_list
            ORDER BY e.id
            """
        ).bindparams(bindparam("ws_list", expanding=True))
    )
    result = await conn.execute(stmt, {"ws_list": list(workshop_ids)})
    return list(result.mappings())


async def _fetch_industry(conn: AsyncConnection, prodclass_id: int) -> Optional[int]:
    stmt = text("SELECT industry_id FROM ib_prodclass WHERE id = :pid")
    result = await conn.execute(stmt, {"pid": prodclass_id})
    row = result.mappings().first()
    if row is None or row.get("industry_id") is None:
        return None
    return int(row["industry_id"])


async def _fetch_prodclass_ids_by_industry(
    conn: AsyncConnection, industry_id: int
) -> List[int]:
    stmt = text(
        """
        SELECT id
        FROM ib_prodclass
        WHERE industry_id = :ind
        ORDER BY id
        """
    )
    result = await conn.execute(stmt, {"ind": industry_id})
    return [int(row["id"]) for row in result.mappings()]


def _apply_equipment_scores(
    equipments: Sequence[Dict[str, Any]],
    score_1: Decimal,
    factor: Decimal,
    acc: Dict[int, EquipmentScore],
    details: List[Dict[str, Any]],
    path: str,
) -> int:
    updated = 0
    for eq in equipments:
        eq_id = int(eq["id"])
        eq_name = eq.get("equipment_name")
        max_score = _to_decimal(eq.get("equipment_score_max"))
        score = _quantize(score_1 * factor * max_score)

        details.append(
            {
                "id": eq_id,
                "equipment_name": eq_name,
                "workshop_id": eq.get("workshop_id"),
                "equipment_score": eq.get("equipment_score"),
                "equipment_score_real": eq.get("equipment_score_real"),
                "equipment_score_max": max_score,
                "SCORE_1": score_1,
                "factor": factor,
                "SCORE_E1": score,
                "path": path,
            }
        )

        existing = acc.get(eq_id)
        if existing is None or score > existing.score:
            acc[eq_id] = EquipmentScore(eq_id, eq_name, score, source="1way")
            updated += 1
        elif existing.name is None and eq_name:
            acc[eq_id] = EquipmentScore(eq_id, eq_name, existing.score, source="1way")
    return updated


async def _compute_equipment_2way(
    conn: AsyncConnection,
    goods_types: Sequence[Dict[str, Any]],
    log_messages: List[str],
) -> Tuple[List[EquipmentScore], List[Dict[str, Any]], List[Dict[str, Any]]]:
    gt_scores: Dict[int, Decimal] = {}
    for row in goods_types:
        if row["goods_type_id"] is None or row["goods_types_score"] is None:
            continue
        gt_id = int(row["goods_type_id"])
        score = _to_decimal(row["goods_types_score"])
        prev = gt_scores.get(gt_id)
        if prev is None or score > prev:
            gt_scores[gt_id] = score

    goods_rows = [
        {"goods_type_id": key, "CRORE_2": value}
        for key, value in sorted(gt_scores.items())
    ]

    if not gt_scores:
        log_messages.append("Шаг 3: SCORE_E2 пропущен — нет goods_type с ненулевым SCORE.")
        return [], goods_rows, []

    stmt = (
        text(
            """
            SELECT
                ieg.equipment_id AS id,
                e.equipment_name,
                e.equipment_score AS core_score,
                g.goods_type_id
            FROM ib_equipment_goods AS ieg
            JOIN ib_goods AS g ON g.id = ieg.goods_id
            JOIN ib_equipment AS e ON e.id = ieg.equipment_id
            WHERE g.goods_type_id IN :gt_list
            """
        ).bindparams(bindparam("gt_list", expanding=True))
    )
    result = await conn.execute(stmt, {"gt_list": list(gt_scores.keys())})
    equipment_rows = result.mappings().all()

    scores: Dict[int, EquipmentScore] = {}
    details: List[Dict[str, Any]] = []
    for row in equipment_rows:
        eq_id = int(row["id"])
        eq_name = row.get("equipment_name")
        goods_type_id = int(row["goods_type_id"])
        crore_2 = gt_scores[goods_type_id]
        crore_3 = _to_decimal(row["core_score"])
        score = _quantize(crore_2 * crore_3)

        details.append(
            {
                "id": eq_id,
                "equipment_name": eq_name,
                "goods_type_id": goods_type_id,
                "CRORE_2": crore_2,
                "CRORE_3": crore_3,
                "SCORE_E2": score,
            }
        )

        existing = scores.get(eq_id)
        if existing is None or score > existing.score:
            scores[eq_id] = EquipmentScore(eq_id, eq_name, score, source="2way")
        elif existing.name is None and eq_name:
            scores[eq_id] = EquipmentScore(eq_id, eq_name, existing.score, source="2way")

    rows_sorted = sorted(scores.values(), key=lambda item: item.id)
    log_messages.append(
        f"Шаг 3: SCORE_E2 рассчитан для {len(rows_sorted)} позиций."
    )
    return rows_sorted, goods_rows, details


async def _compute_equipment_3way(
    conn: AsyncConnection,
    site_equipment: Sequence[Dict[str, Any]],
    log_messages: List[str],
) -> Tuple[List[EquipmentScore], List[Dict[str, Any]]]:
    score_by_equipment: Dict[int, Decimal] = {}
    for row in site_equipment:
        if row["equipment_id"] is None or row["equipment_score"] is None:
            continue
        eq_id = int(row["equipment_id"])
        score = _to_decimal(row["equipment_score"])
        prev = score_by_equipment.get(eq_id)
        if prev is None or score > prev:
            score_by_equipment[eq_id] = _quantize(score)

    details = [
        {
            "equipment_id": eq_id,
            "equipment_score": value,
            "SCORE_E3": value,
        }
        for eq_id, value in sorted(score_by_equipment.items())
    ]

    if not score_by_equipment:
        log_messages.append(
            "Шаг 4: SCORE_E3 пропущен — в ai_site_equipment отсутствуют валидные записи."
        )
        return [], details

    stmt = (
        text(
            """
            SELECT id, equipment_name
            FROM ib_equipment
            WHERE id IN :eq_ids
            """
        ).bindparams(bindparam("eq_ids", expanding=True))
    )
    result = await conn.execute(stmt, {"eq_ids": list(score_by_equipment.keys())})
    name_map = {int(row["id"]): row.get("equipment_name") for row in result.mappings()}

    rows = [
        EquipmentScore(eq_id, name_map.get(eq_id), score, source="3way")
        for eq_id, score in sorted(score_by_equipment.items())
    ]
    log_messages.append(
        f"Шаг 4: SCORE_E3 подготовлен для {len(rows)} элементов оборудования."
    )
    return rows, details


def _merge_equipment_tables(
    eq1: Sequence[EquipmentScore],
    eq2: Sequence[EquipmentScore],
    eq3: Sequence[EquipmentScore],
    log_messages: List[str],
) -> Tuple[List[EquipmentScore], List[Dict[str, Any]]]:
    priority_map = {1: eq1, 2: eq2, 3: eq3}
    combined: Dict[int, Tuple[int, EquipmentScore]] = {}
    merged_rows: List[Dict[str, Any]] = []

    for priority, source_rows in priority_map.items():
        for row in source_rows:
            merged_rows.append(
                {
                    "id": row.id,
                    "equipment_name": row.name,
                    "score": row.score,
                    "source": row.source,
                    "priority": priority,
                }
            )
            existing = combined.get(row.id)
            candidate = (priority, row)
            if existing is None:
                combined[row.id] = candidate
                continue

            prev_priority, prev_row = existing
            if row.score > prev_row.score or (
                row.score == prev_row.score and priority < prev_priority
            ):
                combined[row.id] = candidate

    final_rows = [item[1] for item in sorted(combined.values(), key=lambda x: x[1].id)]
    log_messages.append(
        f"Шаг 5: после объединения осталось {len(final_rows)} уникальных записей."
    )
    return final_rows, merged_rows


async def _sync_equipment_table(
    conn: AsyncConnection,
    table_name: str,
    rows: Sequence[EquipmentScore],
    log_messages: List[str],
) -> None:
    if not rows:
        log_messages.append(
            f"{table_name}: данных нет — существующие записи оставлены без изменений."
        )
        return

    await conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS "{table_name}"(
                id BIGINT PRIMARY KEY,
                equipment_name TEXT,
                score NUMERIC(8,4)
            )
            """
        )
    )

    result = await conn.execute(
        text(f'SELECT id FROM "{table_name}"')
    )
    existing_ids = {int(row["id"]) for row in result.mappings()}
    new_ids = {row.id for row in rows}
    new_records = len(new_ids - existing_ids)

    await conn.execute(
        text(
            f"""
            INSERT INTO "{table_name}"(id, equipment_name, score)
            VALUES (:id, :equipment_name, :score)
            ON CONFLICT (id) DO UPDATE
            SET equipment_name = EXCLUDED.equipment_name,
                score = EXCLUDED.score
            """
        ),
        [
            {
                "id": row.id,
                "equipment_name": row.name,
                "score": row.score,
            }
            for row in rows
        ],
    )

    log_messages.append(
        (
            f"{table_name}: обновлено {len(rows)} записей (новых {new_records}). "
            "Удаление существующих записей не выполнялось."
        )
    )


def _add_table(
    report: EquipmentStepReport,
    *,
    step: str,
    table_id: str,
    title: str,
    rows: Sequence[Any],
    columns: Sequence[str],
) -> None:
    formatted_rows: List[List[Any]] = []
    for row in rows:
        mapping = _row_mapping(row)
        formatted_rows.append([_jsonable(mapping.get(col)) for col in columns])
    report.tables.append(
        TableData(
            step=step,
            table_id=table_id,
            title=title,
            columns=list(columns),
            rows=formatted_rows,
        )
    )


__all__ = [
    "EquipmentSelectionNotFound",
    "EquipmentSelectionResult",
    "build_equipment_tables",
    "compute_equipment_selection",
    "resolve_client_request_id",
]

