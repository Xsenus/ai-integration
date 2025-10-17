"""Сервис расчёта наиболее подходящего оборудования."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.schemas.equipment_selection import (
    ClientRow as ClientRowModel,
    Equipment3WayDetailRow as Equipment3WayDetailModel,
    EquipmentAllRow as EquipmentAllRowModel,
    EquipmentDetailRow as EquipmentDetailModel,
    EquipmentGoodsLinkRow as EquipmentGoodsLinkModel,
    EquipmentSelectionResponse,
    EquipmentWayRow as EquipmentWayRowModel,
    GoodsTypeRow as GoodsTypeRowModel,
    GoodsTypeScoreRow as GoodsTypeScoreModel,
    ProdclassDetail as ProdclassDetailModel,
    ProdclassSourceRow as ProdclassSourceRowModel,
    SampleTable,
    SiteEquipmentRow as SiteEquipmentRowModel,
    WorkshopRow as WorkshopRowModel,
)
log = logging.getLogger("services.equipment_selection")


class EquipmentSelectionNotFound(Exception):
    """Выбрана несуществующая запись клиента."""


_FOUR_DECIMALS = Decimal("0.0001")
_PREVIEW_ROW_LIMIT = 15
_EQUIPMENT_RESULTS_LIMIT = 15
_COLUMN_EXISTS_CACHE: Dict[Tuple[str, str, str], bool] = {}


def _to_decimal(value: object, default: Decimal = Decimal(0)) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _quantize(score: Decimal) -> Decimal:
    return score.quantize(_FOUR_DECIMALS)


def _sort_equipment_rows(rows: Sequence["EquipmentScore"]) -> List["EquipmentScore"]:
    return sorted(rows, key=lambda item: (-item.score, item.id))


def _limit_equipment_rows(
    rows: Sequence["EquipmentScore"], limit: int = _EQUIPMENT_RESULTS_LIMIT
) -> List["EquipmentScore"]:
    sorted_rows = _sort_equipment_rows(rows)
    if limit is None:
        return sorted_rows
    return list(sorted_rows[:limit])


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _format_preview_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        quantized = _quantize(value)
        text = format(quantized.normalize(), "f")
        return text.rstrip("0").rstrip(".") or "0"
    if isinstance(value, float):
        text = f"{value:.4f}"
        return text.rstrip("0").rstrip(".") or "0"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _is_numeric_column(values: Sequence[Any]) -> bool:
    for value in values:
        if value in (None, ""):
            continue
        if isinstance(value, (int, float, Decimal)):
            continue
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                continue
            try:
                Decimal(candidate)
            except Exception:
                return False
            else:
                continue
        return False
    return True


def _align_text(text: str, width: int, align_right: bool) -> str:
    if align_right:
        return text.rjust(width)
    return text.ljust(width)


def _render_table_preview(title: str, columns: Sequence[str], raw_rows: Sequence[Sequence[Any]]) -> str:
    lines: List[str] = [f"=== {title} ==="]
    if not raw_rows:
        lines.append("Пусто.")
        return "\n".join(lines)

    display_rows = [list(row) for row in raw_rows[:_PREVIEW_ROW_LIMIT]]
    formatted: List[List[str]] = []
    for row in display_rows:
        formatted.append([_format_preview_value(value) for value in row])

    columns_list = list(columns)
    widths: List[int] = []
    align_right_flags: List[bool] = []

    for idx, column in enumerate(columns_list):
        col_entries = [row[idx] for row in formatted]
        width = max([len(column)] + [len(entry) for entry in col_entries]) if col_entries else len(column)
        widths.append(width)
        align_right_flags.append(_is_numeric_column([row[idx] for row in display_rows]))

    header = "| " + " | ".join(
        _align_text(columns_list[idx], widths[idx], False) for idx in range(len(columns_list))
    ) + " |"
    separator = "|" + "|".join("-" * (width + 2) for width in widths) + "|"

    lines.append(header)
    lines.append(separator)

    for row in formatted:
        line = "| " + " | ".join(
            _align_text(row[idx], widths[idx], align_right_flags[idx])
            for idx in range(len(columns_list))
        ) + " |"
        lines.append(line)

    if len(raw_rows) > _PREVIEW_ROW_LIMIT:
        remaining = len(raw_rows) - _PREVIEW_ROW_LIMIT
        lines.append(f"... ещё {remaining} строк")

    return "\n".join(lines)


def _log_event(
    log_messages: List[str], message: str, *, level: int = logging.INFO
) -> None:
    """Сохраняет сообщение в отчёт и дублирует его в консольный логгер."""

    log_messages.append(message)
    log.log(level, "equipment-selection: %s", message)


def _append_step_separator(log_messages: List[str], title: str) -> None:
    border = "=" * 18
    _log_event(log_messages, f"{border} {title} {border}", level=logging.INFO)


async def _table_has_column(
    conn: AsyncConnection,
    table_name: str,
    column_name: str,
    *,
    schema: str = "public",
) -> bool:
    """Проверяет наличие колонки в таблице с кешированием результата."""

    cache_key = (schema, table_name, column_name)
    cached = _COLUMN_EXISTS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    stmt = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
          AND column_name = :column
        LIMIT 1
        """
    )
    result = await conn.execute(
        stmt,
        {"schema": schema, "table": table_name, "column": column_name},
    )
    exists = result.scalar() is not None
    _COLUMN_EXISTS_CACHE[cache_key] = exists
    log.debug(
        "equipment-selection: проверка столбца %s.%s.%s → %s",
        schema,
        table_name,
        column_name,
        exists,
    )
    return exists


async def _ensure_table_column(
    conn: AsyncConnection,
    table_name: str,
    column_name: str,
    definition: str,
    *,
    schema: str = "public",
) -> None:
    if await _table_has_column(conn, table_name, column_name, schema=schema):
        log.debug(
            "equipment-selection: столбец %s.%s.%s уже существует",
            schema,
            table_name,
            column_name,
        )
        return

    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    log.info(
        "equipment-selection: добавляем столбец %s.%s.%s (%s)",
        schema,
        table_name,
        column_name,
        definition,
    )
    await conn.execute(
        text(f"ALTER TABLE {qualified} ADD COLUMN {column_name} {definition}"),
    )
    _COLUMN_EXISTS_CACHE[(schema, table_name, column_name)] = True


def _row_mapping(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    if is_dataclass(row):
        return asdict(row)
    if hasattr(row, "_mapping"):
        return dict(row._mapping)  # type: ignore[attr-defined]
    if hasattr(row, "__dict__"):
        return dict(vars(row))
    return dict(row)


def _maybe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _convert_decimals(payload: Dict[str, Any]) -> Dict[str, Any]:
    converted: Dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, Decimal):
            converted[key] = float(value)
        else:
            converted[key] = value
    return converted


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
    raw_rows: List[List[Any]]
    preview: str
    section_title: Optional[str] = None


@dataclass(slots=True)
class EquipmentStepReport:
    company_id: Optional[int] = None
    client_rows: Sequence[Dict[str, Any]] = field(default_factory=list)
    goods_types: Sequence[Dict[str, Any]] = field(default_factory=list)
    site_equipment: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_rows: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_agg: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_paths: Sequence[Dict[str, Any]] = field(default_factory=list)
    prodclass_details: Sequence[Dict[str, Any]] = field(default_factory=list)
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


async def compute_equipment_selection(
    conn: AsyncConnection, client_request_id: int
) -> EquipmentSelectionResponse:
    log.info(
        "equipment-selection: запуск расчёта для clients_requests.id=%s",
        client_request_id,
    )
    report = await build_equipment_tables(conn, client_request_id)
    response = _build_response(report)
    return response


def _create_sample_table(table: TableData) -> SampleTable:
    lines: List[str] = []
    if table.section_title:
        border = "=" * 18
        lines.append(f"{border} {table.section_title} {border}")
        lines.append("")
    lines.extend(table.preview.splitlines())
    return SampleTable(title=table.title, lines=lines)


def _convert_equipment_scores(rows: Sequence[EquipmentScore]) -> List[EquipmentWayRowModel]:
    return [
        EquipmentWayRowModel(
            id=row.id,
            equipment_name=row.name,
            score=float(_to_decimal(row.score)),
        )
        for row in rows
    ]


def _convert_equipment_all(rows: Sequence[EquipmentScore]) -> List[EquipmentAllRowModel]:
    return [
        EquipmentAllRowModel(
            id=row.id,
            equipment_name=row.name,
            score=float(_to_decimal(row.score)),
            source=row.source,
        )
        for row in rows
    ]


def _convert_equipment_detail_list(
    items: Sequence[Any],
) -> List[EquipmentDetailModel]:
    result: List[EquipmentDetailModel] = []
    for item in items:
        mapping = _row_mapping(item)
        eq_id = mapping.get("id")
        if eq_id is None:
            continue
        result.append(
            EquipmentDetailModel(
                id=int(eq_id),
                equipment_name=mapping.get("equipment_name"),
                workshop_id=mapping.get("workshop_id"),
                equipment_score=_maybe_float(mapping.get("equipment_score")),
                equipment_score_real=_maybe_float(mapping.get("equipment_score_real")),
                equipment_score_max=_maybe_float(mapping.get("equipment_score_max")),
                score_1=_maybe_float(mapping.get("SCORE_1") or mapping.get("score_1")),
                factor=_maybe_float(mapping.get("factor")),
                score_e1=_maybe_float(mapping.get("SCORE_E1") or mapping.get("score_e1")),
                path=str(mapping.get("path", "")),
            )
        )
    return result


def _convert_goods_links(items: Sequence[Any]) -> List[EquipmentGoodsLinkModel]:
    result: List[EquipmentGoodsLinkModel] = []
    for item in items:
        mapping = _row_mapping(item)
        equipment_id = mapping.get("id") or mapping.get("equipment_id")
        goods_type_id = mapping.get("goods_type_id")
        if equipment_id is None or goods_type_id is None:
            continue
        result.append(
            EquipmentGoodsLinkModel(
                equipment_id=int(equipment_id),
                goods_type_id=int(goods_type_id),
                crore_2=_maybe_float(mapping.get("CRORE_2") or mapping.get("crore_2")) or 0.0,
                crore_3=_maybe_float(mapping.get("CRORE_3") or mapping.get("crore_3")) or 0.0,
                score_e2=_maybe_float(mapping.get("SCORE_E2") or mapping.get("score_e2")) or 0.0,
                equipment_name=mapping.get("equipment_name"),
            )
        )
    return result


def _convert_goods_type_scores(items: Sequence[Any]) -> List[GoodsTypeScoreModel]:
    result: List[GoodsTypeScoreModel] = []
    for item in items:
        mapping = _row_mapping(item)
        goods_type_id = mapping.get("goods_type_id")
        if goods_type_id is None:
            continue
        result.append(
            GoodsTypeScoreModel(
                goods_type_id=int(goods_type_id),
                crore_2=_maybe_float(mapping.get("CRORE_2") or mapping.get("crore_2")) or 0.0,
            )
        )
    return result


def _convert_equipment_3way_details(
    items: Sequence[Any],
) -> List[Equipment3WayDetailModel]:
    result: List[Equipment3WayDetailModel] = []
    for item in items:
        mapping = _row_mapping(item)
        equipment_id = mapping.get("equipment_id")
        if equipment_id is None:
            continue
        result.append(
            Equipment3WayDetailModel(
                equipment_id=int(equipment_id),
                equipment_score=_maybe_float(mapping.get("equipment_score")),
                score_e3=_maybe_float(mapping.get("SCORE_E3") or mapping.get("score_e3")),
            )
        )
    return result


def _convert_workshops(items: Optional[Sequence[Any]]) -> Optional[List[WorkshopRowModel]]:
    if not items:
        return None
    return [
        WorkshopRowModel(**_convert_decimals(_row_mapping(item)))
        for item in items
    ]


def _convert_prodclass_details(
    items: Sequence[Dict[str, Any]]
) -> List[ProdclassDetailModel]:
    result: List[ProdclassDetailModel] = []
    for item in items:
        mapping = dict(item)
        prodclass_id = mapping.get("prodclass_id")
        if prodclass_id is None:
            continue
        equipment_details = _convert_equipment_detail_list(mapping.get("equipment", []))
        detail = ProdclassDetailModel(
            prodclass_id=int(prodclass_id),
            prodclass_name=mapping.get("prodclass_name"),
            score_1=_maybe_float(mapping.get("score_1")),
            votes=int(mapping.get("votes", 0)),
            path=str(mapping.get("path", "")),
            workshops=_convert_workshops(mapping.get("workshops")) or [],
            fallback_industry_id=mapping.get("fallback_industry_id"),
            fallback_prodclass_ids=mapping.get("fallback_prodclass_ids"),
            fallback_workshops=_convert_workshops(mapping.get("fallback_workshops")),
            equipment=equipment_details,
        )
        result.append(detail)
    return result


def _build_response(report: EquipmentStepReport) -> EquipmentSelectionResponse:
    client_model: Optional[ClientRowModel] = None
    if report.client_rows:
        client_model = ClientRowModel(**_convert_decimals(_row_mapping(report.client_rows[0])))

    goods_types = [
        GoodsTypeRowModel(**_convert_decimals(_row_mapping(row)))
        for row in report.goods_types
    ]
    site_equipment = [
        SiteEquipmentRowModel(**_convert_decimals(_row_mapping(row)))
        for row in report.site_equipment
    ]
    prodclass_rows = [
        ProdclassSourceRowModel(**_convert_decimals(_row_mapping(row)))
        for row in report.prodclass_rows
    ]
    prodclass_details = _convert_prodclass_details(report.prodclass_details)

    goods_type_scores = _convert_goods_type_scores(report.equipment_2way_goods)
    goods_links = _convert_goods_links(report.equipment_2way_details)
    equipment_2way_details = list(goods_links)

    equipment_1way_models = _convert_equipment_scores(report.equipment_1way)
    equipment_2way_models = _convert_equipment_scores(report.equipment_2way)
    equipment_3way_models = _convert_equipment_scores(report.equipment_3way)
    equipment_all_models = _convert_equipment_all(report.equipment_all)

    equipment_1way_details = _convert_equipment_detail_list(report.equipment_1way_details)
    equipment_3way_details = _convert_equipment_3way_details(report.equipment_3way_details)

    sample_tables = [_create_sample_table(table) for table in report.tables]

    return EquipmentSelectionResponse(
        client=client_model,
        goods_types=goods_types,
        site_equipment=site_equipment,
        prodclass_rows=prodclass_rows,
        prodclass_details=prodclass_details,
        goods_type_scores=goods_type_scores,
        goods_links=goods_links,
        equipment_1way=equipment_1way_models,
        equipment_1way_details=equipment_1way_details,
        equipment_2way=equipment_2way_models,
        equipment_2way_details=equipment_2way_details,
        equipment_3way=equipment_3way_models,
        equipment_3way_details=equipment_3way_details,
        equipment_all=equipment_all_models,
        sample_tables=sample_tables,
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
    _log_event(
        report.log,
        f"Старт расчёта оборудования для clients_requests.id={client_request_id}.",
    )

    log.debug(
        "equipment-selection: начинаем сбор данных для clients_requests.id=%s",
        client_request_id,
    )

    _append_step_separator(report.log, "Шаг 0 — Клиент")
    client_rows = await _load_client(conn, client_request_id)
    report.client_rows = client_rows
    report.company_id = await _resolve_company_id(
        conn, client_request_id, client_rows, report.log
    )
    client_columns = [
        "id",
        "company_name",
        "inn",
        "domain_1",
        "started_at",
        "ended_at",
    ]
    _add_table(
        report,
        step="0",
        table_id="client",
        title=f"Клиент (clients_requests.id={client_request_id})",
        rows=client_rows,
        columns=client_columns,
        section_title="Шаг 0 — Клиент",
    )
    _log_event(
        report.log,
        f"Шаг 0: загружена карточка клиента (строк: {len(client_rows)}).",
    )

    _append_step_separator(report.log, "Шаг 1 — Данные с сайта")
    goods_types = await _load_goods_types(conn, report.company_id)
    report.goods_types = goods_types
    _add_table(
        report,
        step="1.a",
        table_id="goods_types",
        title="1.a) Типы продукции (ai_site_goods_types.goods_type)",
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
        section_title="Шаг 1 — Данные с сайта",
    )
    _log_event(
        report.log,
        f"Шаг 1.a: найдено {len(goods_types)} записей ai_site_goods_types.",
    )

    site_equipment = await _load_site_equipment(conn, report.company_id)
    report.site_equipment = site_equipment
    _add_table(
        report,
        step="1.b",
        table_id="site_equipment",
        title="1.b) Оборудование с сайта (ai_site_equipment.equipment)",
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
    _log_event(
        report.log,
        f"Шаг 1.b: найдено {len(site_equipment)} элементов оборудования с сайта.",
    )

    _append_step_separator(report.log, "Шаг 2 — SCORE_E1 через prodclass")
    prodclass_rows = await _load_prodclass_rows(conn, report.company_id)
    report.prodclass_rows = prodclass_rows
    _add_table(
        report,
        step="2.a",
        table_id="prodclass_rows",
        title="2.a) ai_site_prodclass (все записи клиента)",
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
        section_title="Шаг 2 — SCORE_E1 через prodclass",
    )
    _log_event(
        report.log,
        f"Шаг 2.a: загружено {len(prodclass_rows)} записей ai_site_prodclass.",
    )

    prodclass_agg = _aggregate_prodclass(prodclass_rows)
    report.prodclass_agg = prodclass_agg
    _add_table(
        report,
        step="2.b",
        table_id="prodclass_agg",
        title="2.b) Средний prodclass_score (SCORE_1) по каждому prodclass",
        rows=prodclass_agg,
        columns=["prodclass_id", "prodclass_name", "SCORE_1", "votes"],
    )
    _log_event(
        report.log,
        f"Шаг 2.b: агрегировано {len(prodclass_agg)} prodclass со средним SCORE_1.",
    )

    (
        equipment_1way,
        path_log,
        equipment_1way_details,
        prodclass_details,
    ) = await _compute_equipment_1way(conn, prodclass_agg, report.log)
    equipment_1way = _limit_equipment_rows(equipment_1way)
    report.equipment_1way = equipment_1way
    report.prodclass_paths = path_log
    report.equipment_1way_details = equipment_1way_details
    report.prodclass_details = prodclass_details

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
    equipment_1way_table = [
        {"id": item.id, "equipment_name": item.name, "score": item.score}
        for item in equipment_1way
    ]
    _add_table(
        report,
        step="2.d",
        table_id="equipment_1way",
        title="2.d) EQUIPMENT_1way (ID, equipment_name, SCORE)",
        rows=equipment_1way_table,
        columns=["id", "equipment_name", "score"],
    )

    _append_step_separator(report.log, "Шаг 3 — SCORE_E2 через goods_type")
    (
        equipment_2way,
        goods_type_scores,
        equipment_2way_details,
    ) = await _compute_equipment_2way(conn, goods_types, report.log)
    equipment_2way = _limit_equipment_rows(equipment_2way)
    report.equipment_2way = equipment_2way
    report.equipment_2way_goods = goods_type_scores
    report.equipment_2way_details = equipment_2way_details

    _add_table(
        report,
        step="3.a",
        table_id="equipment_2way_goods",
        title="3.a) ID_4 = goods_type_ID и CRORE_2 = goods_types_score",
        rows=goods_type_scores,
        columns=["goods_type_id", "CRORE_2"],
        section_title="Шаг 3 — SCORE_E2 через goods_type",
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
    equipment_2way_table = [
        {"id": item.id, "equipment_name": item.name, "score": item.score}
        for item in equipment_2way
    ]
    _add_table(
        report,
        step="3.d",
        table_id="equipment_2way",
        title="3.d) EQUIPMENT_2way (ID, equipment_name, SCORE)",
        rows=equipment_2way_table,
        columns=["id", "equipment_name", "score"],
    )

    _append_step_separator(report.log, "Шаг 4 — SCORE_E3 через ai_site_equipment")
    equipment_3way, equipment_3way_details = await _compute_equipment_3way(
        conn, site_equipment, report.log
    )
    equipment_3way = _limit_equipment_rows(equipment_3way)
    report.equipment_3way = equipment_3way
    report.equipment_3way_details = equipment_3way_details

    _add_table(
        report,
        step="4.a",
        table_id="equipment_3way_details",
        title="4.a) ai_site_equipment (equipment_ID, equipment_score, SCORE_E3)",
        rows=equipment_3way_details,
        columns=["equipment_id", "equipment_score", "SCORE_E3"],
        section_title="Шаг 4 — SCORE_E3 через ai_site_equipment",
    )
    equipment_3way_table = [
        {"id": item.id, "equipment_name": item.name, "score": item.score}
        for item in equipment_3way
    ]
    _add_table(
        report,
        step="4.b",
        table_id="equipment_3way",
        title="4.b) EQUIPMENT_3way (ID, equipment_name, SCORE)",
        rows=equipment_3way_table,
        columns=["id", "equipment_name", "score"],
    )

    _append_step_separator(report.log, "Шаг 5 — Сборка EQUIPMENT_ALL")
    equipment_all, equipment_all_sources = _merge_equipment_tables(
        equipment_1way, equipment_2way, equipment_3way, report.log
    )
    equipment_all = _limit_equipment_rows(equipment_all)
    report.equipment_all = equipment_all
    report.equipment_all_sources = equipment_all_sources

    _add_table(
        report,
        step="5.a",
        table_id="equipment_all_sources",
        title="5.a) Объединённый список до очистки",
        rows=equipment_all_sources,
        columns=["id", "equipment_name", "score", "source", "priority"],
        section_title="Шаг 5 — Сборка EQUIPMENT_ALL",
    )
    equipment_all_table = [
        {"id": item.id, "equipment_name": item.name, "score": item.score, "source": item.source}
        for item in equipment_all
    ]
    _add_table(
        report,
        step="5.b",
        table_id="equipment_all",
        title="5.b) EQUIPMENT_ALL (после дедупликации)",
        rows=equipment_all_table,
        columns=["id", "equipment_name", "score", "source"],
    )

    await _sync_equipment_table(
        conn,
        "EQUIPMENT_1way",
        equipment_1way,
        client_request_id,
        report.log,
    )
    await _sync_equipment_table(
        conn,
        "EQUIPMENT_2way",
        equipment_2way,
        client_request_id,
        report.log,
    )
    await _sync_equipment_table(
        conn,
        "EQUIPMENT_3way",
        equipment_3way,
        client_request_id,
        report.log,
    )
    await _sync_equipment_table(
        conn,
        "EQUIPMENT_ALL",
        equipment_all,
        client_request_id,
        report.log,
    )

    _append_step_separator(report.log, "Итоговый отчёт")
    _log_event(
        report.log,
        "Расчёт оборудования завершён. Обновлены таблицы EQUIPMENT_* в базе данных.",
    )
    log.info(
        "equipment-selection: расчёт завершён для clients_requests.id=%s",
        client_request_id,
    )
    return report


async def _load_client(
    conn: AsyncConnection, client_request_id: int
) -> List[Dict[str, Any]]:
    log.debug(
        "equipment-selection: загружаем клиента из public.clients_requests (id=%s)",
        client_request_id,
    )
    select_columns: List[str] = [
        "id",
        "company_name",
        "inn",
        "domain_1",
        "started_at",
        "ended_at",
    ]

    stmt = text(
        f"""
        SELECT {', '.join(select_columns)}
        FROM public.clients_requests
        WHERE id = :cid
        """
    )
    log.debug(
        "equipment-selection: выполняем запрос клиента → %s",
        stmt.text,
    )
    result = await conn.execute(stmt, {"cid": client_request_id})
    mappings = list(result.mappings())
    if not mappings:
        raise EquipmentSelectionNotFound(
            f"clients_requests.id={client_request_id} не найден"
        )

    rows: List[Dict[str, Any]] = []
    rows = [dict(item) for item in mappings]

    log.info(
        "equipment-selection: загружена карточка клиента id=%s (строк=%s)",
        client_request_id,
        len(rows),
    )

    return rows


async def _resolve_company_id(
    conn: AsyncConnection,
    client_request_id: int,
    client_rows: Sequence[Dict[str, Any]],
    log_messages: List[str],
) -> int:
    resolved = client_request_id

    if client_rows:
        _log_event(
            log_messages,
            (
                "clients_requests: используем первичный ключ id как company_id. "
                "Отдельная колонка company_id не требуется и не заполняется."
            ),
        )
    else:
        _log_event(
            log_messages,
            "clients_requests: строка не найдена, расчёт завершится ошибкой выше по стеку.",
            level=logging.WARNING,
        )

    stmt = text(
        """
        SELECT COUNT(*) AS rows_count
        FROM public.pars_site
        WHERE company_id = :cid
        """
    )
    log.debug(
        "equipment-selection: проверяем pars_site на наличие company_id=%s",
        resolved,
    )
    result = await conn.execute(stmt, {"cid": resolved})
    row = result.mappings().first()
    rows_count = 0
    if row:
        mapping = dict(row)
        raw_count = mapping.get("rows_count")
        if raw_count is not None:
            rows_count = int(raw_count)

    if rows_count:
        _log_event(
            log_messages,
            (
                "pars_site: найдено "
                f"{rows_count} записей с company_id={resolved} (совпадает с clients_requests.id)."
            ),
        )
    else:
        _log_event(
            log_messages,
            (
                "pars_site: записи с company_id="
                f"{resolved} не найдены. Проверьте синхронизацию pars_site для клиента."
            ),
            level=logging.WARNING,
        )

    _log_event(
        log_messages,
        (
            "Итоговое значение company_id для расчёта: "
            f"{resolved} (источник: clients_requests.id)."
        ),
    )
    return resolved


async def _load_goods_types(
    conn: AsyncConnection,
    company_id: int,
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
    log.debug(
        "equipment-selection: загружаем goods_types → %s",
        stmt.text,
    )
    result = await conn.execute(stmt, {"cid": company_id})
    rows = list(result.mappings())
    log.info(
        "equipment-selection: получено %s строк goods_types для company_id=%s",
        len(rows),
        company_id,
    )
    return rows


async def _load_site_equipment(
    conn: AsyncConnection,
    company_id: int,
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
    log.debug(
        "equipment-selection: загружаем site_equipment → %s",
        stmt.text,
    )
    result = await conn.execute(stmt, {"cid": company_id})
    rows = list(result.mappings())
    log.info(
        "equipment-selection: получено %s строк site_equipment для company_id=%s",
        len(rows),
        company_id,
    )
    return rows


async def _load_prodclass_rows(
    conn: AsyncConnection,
    company_id: int,
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
    log.debug(
        "equipment-selection: загружаем prodclass_rows → %s",
        stmt.text,
    )
    result = await conn.execute(stmt, {"cid": company_id})
    rows = list(result.mappings())
    log.info(
        "equipment-selection: получено %s строк prodclass для company_id=%s",
        len(rows),
        company_id,
    )
    return rows


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
) -> Tuple[
    List[EquipmentScore],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    log.info(
        "equipment-selection: запускаем расчёт SCORE_E1 (prodclass_agg=%s)",
        len(prodclass_agg),
    )
    equipment_rows: Dict[int, EquipmentScore] = {}
    path_log: List[Dict[str, Any]] = []
    details: List[Dict[str, Any]] = []
    prodclass_details: List[Dict[str, Any]] = []
    direct_updates = 0
    fallback_updates = 0

    for row in prodclass_agg:
        prodclass_id = int(row["prodclass_id"])
        score_1 = _to_decimal(row["SCORE_1"])
        pc_name = row.get("prodclass_name")
        votes = int(row.get("votes", 0)) if row.get("votes") is not None else 0

        detail_entry: Dict[str, Any] = {
            "prodclass_id": prodclass_id,
            "prodclass_name": pc_name,
            "score_1": score_1,
            "votes": votes,
            "path": "",
            "workshops": [],
            "fallback_industry_id": None,
            "fallback_prodclass_ids": None,
            "fallback_workshops": None,
            "equipment": [],
        }

        workshops = await _fetch_workshops(conn, [prodclass_id])
        if workshops:
            workshop_dicts = [_row_mapping(w) for w in workshops]
            detail_entry["path"] = "direct"
            detail_entry["workshops"] = workshop_dicts
            equipments = await _fetch_equipment_by_workshops(
                conn, [int(w["id"]) for w in workshop_dicts]
            )
            before_details = len(details)
            updated = _apply_equipment_scores(
                equipments,
                score_1,
                Decimal("1"),
                equipment_rows,
                details,
                path="direct",
            )
            new_equipment = [
                dict(details[idx]) for idx in range(before_details, len(details))
            ]
            detail_entry["equipment"] = new_equipment
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
            prodclass_details.append(detail_entry)
            continue

        industry_id = await _fetch_industry(conn, prodclass_id)
        if industry_id is None:
            detail_entry["path"] = "fallback_missing_industry"
            path_log.append(
                {
                    "prodclass_id": prodclass_id,
                    "prodclass_name": pc_name,
                    "path": "fallback_missing_industry",
                    "workshops": 0,
                    "equipment": 0,
                }
            )
            prodclass_details.append(detail_entry)
            continue

        related_prodclasses = await _fetch_prodclass_ids_by_industry(conn, industry_id)
        workshops_fb = await _fetch_workshops(conn, related_prodclasses)
        if not workshops_fb:
            detail_entry["path"] = "fallback_no_workshops"
            detail_entry["fallback_industry_id"] = industry_id
            detail_entry["fallback_prodclass_ids"] = related_prodclasses
            path_log.append(
                {
                    "prodclass_id": prodclass_id,
                    "prodclass_name": pc_name,
                    "path": "fallback_no_workshops",
                    "workshops": 0,
                    "equipment": 0,
                }
            )
            prodclass_details.append(detail_entry)
            continue

        fallback_workshop_dicts = [_row_mapping(w) for w in workshops_fb]
        detail_entry["path"] = "fallback"
        detail_entry["fallback_industry_id"] = industry_id
        detail_entry["fallback_prodclass_ids"] = related_prodclasses
        detail_entry["fallback_workshops"] = fallback_workshop_dicts
        equipments_fb = await _fetch_equipment_by_workshops(
            conn, [int(w["id"]) for w in fallback_workshop_dicts]
        )
        before_details = len(details)
        updated = _apply_equipment_scores(
            equipments_fb,
            score_1,
            Decimal("0.75"),
            equipment_rows,
            details,
            path="fallback",
        )
        new_equipment = [
            dict(details[idx]) for idx in range(before_details, len(details))
        ]
        detail_entry["equipment"] = new_equipment
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
        prodclass_details.append(detail_entry)

    rows_sorted = _sort_equipment_rows(equipment_rows.values())
    _log_event(
        log_messages,
        "Шаг 2: SCORE_E1 рассчитан для "
        f"{len(rows_sorted)} позиций (direct={direct_updates}, fallback={fallback_updates}).",
    )
    return rows_sorted, path_log, details, prodclass_details


async def _fetch_workshops(
    conn: AsyncConnection, prodclass_ids: Sequence[int]
) -> List[Dict[str, Any]]:
    if not prodclass_ids:
        return []
    log.debug(
        "equipment-selection: загружаем workshops для prodclass_ids=%s",
        prodclass_ids,
    )
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
    log.debug(
        "equipment-selection: загружаем оборудование для workshop_ids=%s",
        workshop_ids,
    )
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
    log.debug(
        "equipment-selection: ищем industry для prodclass_id=%s",
        prodclass_id,
    )
    stmt = text("SELECT industry_id FROM ib_prodclass WHERE id = :pid")
    result = await conn.execute(stmt, {"pid": prodclass_id})
    row = result.mappings().first()
    if row is None or row.get("industry_id") is None:
        return None
    return int(row["industry_id"])


async def _fetch_prodclass_ids_by_industry(
    conn: AsyncConnection, industry_id: int
) -> List[int]:
    log.debug(
        "equipment-selection: ищем prodclass по industry_id=%s",
        industry_id,
    )
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
    log.info(
        "equipment-selection: запускаем расчёт SCORE_E2 (goods_types=%s)",
        len(goods_types),
    )
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
        _log_event(
            log_messages,
            "Шаг 3: SCORE_E2 пропущен — нет goods_type с ненулевым SCORE.",
            level=logging.WARNING,
        )
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
    gt_list = list(gt_scores.keys())
    log.debug(
        "equipment-selection: запрашиваем SCORE_E2 для goods_type_ids=%s",
        gt_list,
    )
    result = await conn.execute(stmt, {"gt_list": gt_list})
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

    rows_sorted = _sort_equipment_rows(scores.values())
    _log_event(
        log_messages,
        f"Шаг 3: SCORE_E2 рассчитан для {len(rows_sorted)} позиций.",
    )
    return rows_sorted, goods_rows, details


async def _compute_equipment_3way(
    conn: AsyncConnection,
    site_equipment: Sequence[Dict[str, Any]],
    log_messages: List[str],
) -> Tuple[List[EquipmentScore], List[Dict[str, Any]]]:
    log.info(
        "equipment-selection: запускаем расчёт SCORE_E3 (site_equipment=%s)",
        len(site_equipment),
    )
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
        _log_event(
            log_messages,
            "Шаг 4: SCORE_E3 пропущен — в ai_site_equipment отсутствуют валидные записи.",
            level=logging.WARNING,
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
    eq_ids = list(score_by_equipment.keys())
    log.debug(
        "equipment-selection: загружаем названия оборудования для %s идентификаторов",
        len(eq_ids),
    )
    result = await conn.execute(stmt, {"eq_ids": eq_ids})
    name_map = {int(row["id"]): row.get("equipment_name") for row in result.mappings()}

    rows = [
        EquipmentScore(eq_id, name_map.get(eq_id), score, source="3way")
        for eq_id, score in sorted(score_by_equipment.items())
    ]
    rows = _sort_equipment_rows(rows)
    _log_event(
        log_messages,
        f"Шаг 4: SCORE_E3 подготовлен для {len(rows)} элементов оборудования.",
    )
    return rows, details


def _merge_equipment_tables(
    eq1: Sequence[EquipmentScore],
    eq2: Sequence[EquipmentScore],
    eq3: Sequence[EquipmentScore],
    log_messages: List[str],
) -> Tuple[List[EquipmentScore], List[Dict[str, Any]]]:
    log.info(
        "equipment-selection: объединяем таблицы (1way=%s, 2way=%s, 3way=%s)",
        len(eq1),
        len(eq2),
        len(eq3),
    )
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

    final_rows = _sort_equipment_rows([entry[1] for entry in combined.values()])
    _log_event(
        log_messages,
        f"Шаг 5: после объединения осталось {len(final_rows)} уникальных записей.",
    )
    return final_rows, merged_rows


async def _sync_equipment_table(
    conn: AsyncConnection,
    table_name: str,
    rows: Sequence[EquipmentScore],
    client_request_id: int,
    log_messages: List[str],
) -> None:
    owner_id = client_request_id
    log.info(
        "equipment-selection: начинаем синхронизацию таблицы %s (строк для записи=%s, clients_requests.id=%s)",
        table_name,
        len(rows),
        owner_id,
    )
    await conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS "{table_name}"(
                id BIGINT PRIMARY KEY,
                company_id BIGINT,
                equipment_name TEXT,
                score NUMERIC(8,4),
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            )
            """
        )
    )

    await _ensure_table_column(conn, table_name, "company_id", "BIGINT")
    await _ensure_table_column(conn, table_name, "equipment_name", "TEXT")
    await _ensure_table_column(conn, table_name, "score", "NUMERIC(8,4)")
    await _ensure_table_column(conn, table_name, "created_at", "TIMESTAMPTZ")
    await _ensure_table_column(conn, table_name, "updated_at", "TIMESTAMPTZ")

    if not rows:
        _log_event(
            log_messages,
            f"{table_name}: данных нет — существующие записи оставлены без изменений.",
        )
        return

    result = await conn.execute(
        text(
            f"""
            SELECT id, company_id, equipment_name, score, created_at, updated_at
            FROM "{table_name}"
            """
        )
    )
    existing_map: Dict[int, Dict[str, Any]] = {}
    for record in result.mappings():
        existing_map[int(record["id"])] = {
            "company_id": record.get("company_id"),
            "equipment_name": record.get("equipment_name"),
            "score": _quantize(_to_decimal(record.get("score"))),
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
        }

    log.debug(
        "equipment-selection: в таблице %s уже %s записей",
        table_name,
        len(existing_map),
    )

    unique_rows: Dict[int, EquipmentScore] = {}
    for item in rows:
        if item.id not in unique_rows:
            unique_rows[item.id] = item
    deduped_rows = list(unique_rows.values())

    now = datetime.now(timezone.utc)
    updates: List[Dict[str, Any]] = []
    inserts: List[Dict[str, Any]] = []

    for row in deduped_rows:
        new_score = _quantize(_to_decimal(row.score))
        payload = {
            "id": row.id,
            "company_id": owner_id,
            "equipment_name": row.name,
            "score": new_score,
            "created_at": now,
            "updated_at": now,
        }
        current = existing_map.get(row.id)
        if current is None:
            inserts.append(payload)
            continue

        needs_update = (
            current.get("equipment_name") != row.name
            or current.get("score") != new_score
            or current.get("company_id") != owner_id
            or current.get("created_at") is None
        )

        if needs_update or current.get("updated_at") is None:
            payload["created_at"] = current.get("created_at") or now
            updates.append(payload)

    if inserts:
        await conn.execute(
            text(
                f"""
                INSERT INTO "{table_name}"(id, company_id, equipment_name, score, created_at, updated_at)
                VALUES (:id, :company_id, :equipment_name, :score, :created_at, :updated_at)
                """
            ),
            inserts,
        )
        log.info(
            "equipment-selection: в таблицу %s вставлено %s новых строк",
            table_name,
            len(inserts),
        )

    if updates:
        await conn.execute(
            text(
                f"""
                UPDATE "{table_name}"
                SET company_id = :company_id,
                    equipment_name = :equipment_name,
                    score = :score,
                    created_at = COALESCE(created_at, :created_at),
                    updated_at = :updated_at
                WHERE id = :id
                """
            ),
            updates,
        )
        log.info(
            "equipment-selection: в таблице %s обновлено %s строк",
            table_name,
            len(updates),
        )

    _log_event(
        log_messages,
        (
            f"{table_name}: обработано {len(deduped_rows)} записей ("
            f"добавлено {len(inserts)}, обновлено {len(updates)}). "
            "Удаление существующих записей не выполнялось."
        ),
    )


def _add_table(
    report: EquipmentStepReport,
    *,
    step: str,
    table_id: str,
    title: str,
    rows: Sequence[Any],
    columns: Sequence[str],
    section_title: Optional[str] = None,
) -> None:
    formatted_rows: List[List[Any]] = []
    raw_rows: List[List[Any]] = []
    for row in rows:
        mapping = _row_mapping(row)
        raw_row = [mapping.get(col) for col in columns]
        raw_rows.append(raw_row)
        formatted_rows.append([_jsonable(value) for value in raw_row])
    preview = _render_table_preview(title, columns, raw_rows)
    table = TableData(
        step=step,
        table_id=table_id,
        title=title,
        columns=list(columns),
        rows=formatted_rows,
        raw_rows=raw_rows,
        preview=preview,
        section_title=section_title,
    )
    report.tables.append(table)
    log.info(
        "equipment-selection: собрана таблица %s (%s) — %s строк, колонки: %s",
        table_id,
        title,
        len(rows),
        ", ".join(columns),
    )
    for line in table.preview.splitlines():
        log.debug("equipment-selection: %s preview | %s", table_id, line)


__all__ = [
    "EquipmentSelectionNotFound",
    "build_equipment_tables",
    "compute_equipment_selection",
    "resolve_client_request_id",
]

