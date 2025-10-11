"""Business logic for the equipment selection feature."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pydantic import BaseModel, Field
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncConnection

log = logging.getLogger("services.equipment_selection")


class EquipmentSelectionNotFound(Exception):
    """Raised when the requested client or INN does not exist."""


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive branch
        return None


def _row_to_dict(row: Any) -> Dict[str, Any]:
    mapping = getattr(row, "_mapping", row)
    return {key: mapping[key] for key in mapping.keys()}


class ClientInfo(BaseModel):
    id: int
    company_name: Optional[str] = None
    inn: Optional[str] = None
    domain_1: Optional[str] = None
    domain_2: Optional[str] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None


class GoodsTypeItem(BaseModel):
    id: int
    goods_type: Optional[str] = None
    goods_type_id: Optional[int] = None
    goods_types_score: Optional[float] = None
    text_par_id: Optional[int] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None


class SiteEquipmentItem(BaseModel):
    id: int
    equipment: Optional[str] = None
    equipment_id: Optional[int] = None
    equipment_score: Optional[float] = None
    text_pars_id: Optional[int] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None


class ProdclassRow(BaseModel):
    prodclass_id: int
    prodclass_name: Optional[str] = None
    prodclass_score: Optional[float] = None
    text_pars_id: Optional[int] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None


class ProdclassDetail(BaseModel):
    prodclass_id: int
    prodclass_name: Optional[str] = None
    score_1: float
    votes: int
    path: str
    workshops_count: int
    equipment_count: int
    industry_id: Optional[int] = None


class EquipmentScore(BaseModel):
    id: int
    equipment_name: Optional[str] = None
    score: float
    source: Optional[str] = None


class EquipmentSelectionResult(BaseModel):
    client: Optional[ClientInfo] = None
    goods_types: List[GoodsTypeItem] = Field(default_factory=list)
    site_equipment: List[SiteEquipmentItem] = Field(default_factory=list)
    prodclass_rows: List[ProdclassRow] = Field(default_factory=list)
    prodclass_details: List[ProdclassDetail] = Field(default_factory=list)
    equipment_1way: List[EquipmentScore] = Field(default_factory=list)
    equipment_2way: List[EquipmentScore] = Field(default_factory=list)
    equipment_3way: List[EquipmentScore] = Field(default_factory=list)
    equipment_all: List[EquipmentScore] = Field(default_factory=list)


@dataclass
class _EquipmentEntry:
    score: float
    equipment_name: Optional[str]
    source: str


async def _fetch_client(conn: AsyncConnection, client_id: int) -> ClientInfo:
    log.info("equipment-selection: fetching client %s", client_id)
    sql = text(
        """
        SELECT id, company_name, inn, domain_1, domain_2, started_at, ended_at
        FROM public.clients_requests
        WHERE id = :cid
        """
    )
    row = (await conn.execute(sql, {"cid": client_id})).first()
    if row is None:
        log.warning("equipment-selection: client %s not found", client_id)
        raise EquipmentSelectionNotFound(f"clients_requests.id={client_id} not found")
    data = _row_to_dict(row)
    log.debug("equipment-selection: fetched client data %s", data)
    return ClientInfo(**data)


async def _fetch_goods_types(conn: AsyncConnection, client_id: int) -> List[GoodsTypeItem]:
    log.info("equipment-selection: loading goods types for client %s", client_id)
    sql = text(
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
    rows = await conn.execute(sql, {"cid": client_id})
    items: List[GoodsTypeItem] = []
    for row in rows:
        data = _row_to_dict(row)
        score = _as_float(data.get("goods_types_score"))
        data["goods_types_score"] = score
        items.append(GoodsTypeItem(**data))
    log.info(
        "equipment-selection: loaded %s goods types for client %s", len(items), client_id
    )
    log.debug(
        "equipment-selection: goods types payload %s",
        [item.model_dump() for item in items],
    )
    return items


async def _fetch_site_equipment(conn: AsyncConnection, client_id: int) -> List[SiteEquipmentItem]:
    log.info("equipment-selection: loading site equipment for client %s", client_id)
    sql = text(
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
    rows = await conn.execute(sql, {"cid": client_id})
    items: List[SiteEquipmentItem] = []
    for row in rows:
        data = _row_to_dict(row)
        score = _as_float(data.get("equipment_score"))
        data["equipment_score"] = score
        items.append(SiteEquipmentItem(**data))
    log.info(
        "equipment-selection: loaded %s site equipment items for client %s",
        len(items),
        client_id,
    )
    log.debug(
        "equipment-selection: site equipment payload %s",
        [item.model_dump() for item in items],
    )
    return items


async def _fetch_prodclass_rows(conn: AsyncConnection, client_id: int) -> List[ProdclassRow]:
    log.info("equipment-selection: loading prodclass rows for client %s", client_id)
    sql = text(
        """
        SELECT
            ap.prodclass,
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
    rows = await conn.execute(sql, {"cid": client_id})
    items: List[ProdclassRow] = []
    for row in rows:
        data = _row_to_dict(row)
        data["prodclass_id"] = int(data.pop("prodclass"))
        data["prodclass_score"] = _as_float(data.get("prodclass_score"))
        items.append(ProdclassRow(**data))
    log.info(
        "equipment-selection: loaded %s prodclass rows for client %s",
        len(items),
        client_id,
    )
    log.debug(
        "equipment-selection: prodclass rows payload %s",
        [item.model_dump() for item in items],
    )
    return items


async def _fetch_equipment_for_workshops(
    conn: AsyncConnection, workshop_ids: Sequence[int]
) -> List[dict]:
    if not workshop_ids:
        log.info("equipment-selection: equipment request skipped, no workshop ids provided")
        return []
    log.info(
        "equipment-selection: loading equipment for workshops %s",
        list(workshop_ids),
    )
    sql = (
        text(
            """
            SELECT
                e.id,
                e.equipment_name,
                e.workshop_id,
                e.equipment_score,
                e.equipment_score_real
            FROM ib_equipment AS e
            WHERE e.workshop_id IN :ws_list
            ORDER BY e.id
            """
        ).bindparams(bindparam("ws_list", expanding=True))
    )
    rows = await conn.execute(sql, {"ws_list": list(workshop_ids)})
    data: List[dict] = []
    for row in rows:
        rec = _row_to_dict(row)
        rec["equipment_score"] = _as_float(rec.get("equipment_score"))
        rec["equipment_score_real"] = _as_float(rec.get("equipment_score_real"))
        data.append(rec)
    log.info(
        "equipment-selection: loaded %s equipment rows for workshops %s",
        len(data),
        list(workshop_ids),
    )
    log.debug("equipment-selection: equipment rows payload %s", data)
    return data


async def _fetch_workshops(conn: AsyncConnection, prodclass_ids: Sequence[int]) -> List[dict]:
    if not prodclass_ids:
        log.info("equipment-selection: workshop request skipped, no prodclass ids provided")
        return []
    log.info("equipment-selection: loading workshops for prodclasses %s", list(prodclass_ids))
    sql = (
        text(
            """
            SELECT id, workshop_name, workshop_score, prodclass_id, company_id
            FROM ib_workshops
            WHERE prodclass_id IN :pc_list
            ORDER BY id
            """
        ).bindparams(bindparam("pc_list", expanding=True))
    )
    rows = await conn.execute(sql, {"pc_list": list(prodclass_ids)})
    data: List[dict] = []
    for row in rows:
        rec = _row_to_dict(row)
        rec["workshop_score"] = _as_float(rec.get("workshop_score"))
        data.append(rec)
    log.info(
        "equipment-selection: loaded %s workshops for prodclasses %s",
        len(data),
        list(prodclass_ids),
    )
    log.debug("equipment-selection: workshop rows payload %s", data)
    return data


async def _fetch_industry_id(conn: AsyncConnection, prodclass_id: int) -> Optional[int]:
    log.info(
        "equipment-selection: resolving industry id for prodclass %s", prodclass_id
    )
    sql = text("SELECT industry_id FROM ib_prodclass WHERE id = :pid")
    row = (await conn.execute(sql, {"pid": prodclass_id})).first()
    if row is None:
        log.info(
            "equipment-selection: no industry found for prodclass %s", prodclass_id
        )
        return None
    value = row[0]
    if value is None:
        log.info(
            "equipment-selection: prodclass %s has NULL industry", prodclass_id
        )
        return None
    log.info(
        "equipment-selection: prodclass %s mapped to industry %s", prodclass_id, value
    )
    return int(value)


def _aggregate_prodclass(rows: Iterable[ProdclassRow]) -> List[ProdclassDetail]:
    log.info("equipment-selection: aggregating prodclass rows")
    grouped: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        key = row.prodclass_id
        entry = grouped.setdefault(
            key,
            {
                "prodclass_id": key,
                "prodclass_name": row.prodclass_name,
                "score_sum": 0.0,
                "votes": 0,
            },
        )
        score = row.prodclass_score or 0.0
        entry["score_sum"] += score
        entry["votes"] += 1
    details: List[ProdclassDetail] = []
    for entry in grouped.values():
        votes = entry["votes"] or 1
        avg = entry["score_sum"] / votes
        details.append(
            ProdclassDetail(
                prodclass_id=entry["prodclass_id"],
                prodclass_name=entry.get("prodclass_name"),
                score_1=round(avg, 4),
                votes=votes,
                path="skipped",
                workshops_count=0,
                equipment_count=0,
                industry_id=None,
            )
        )
    details.sort(key=lambda x: (-x.score_1, -x.votes, x.prodclass_id))
    log.info(
        "equipment-selection: aggregated prodclass details count=%s",
        len(details),
    )
    log.debug(
        "equipment-selection: prodclass details payload %s",
        [detail.model_dump() for detail in details],
    )
    return details


def _calc_equipment_scores(
    equipment_rows: Iterable[dict],
    score_multiplier: float,
) -> Dict[int, _EquipmentEntry]:
    result: Dict[int, _EquipmentEntry] = {}
    for row in equipment_rows:
        equipment_id = int(row["id"])
        equipment_name = row.get("equipment_name")
        score_base = max(
            (row.get("equipment_score") or 0.0),
            (row.get("equipment_score_real") or 0.0),
        )
        score = round(score_multiplier * (score_base or 0.0), 4)
        if score <= 0:
            continue
        current = result.get(equipment_id)
        entry = _EquipmentEntry(score=score, equipment_name=equipment_name, source="1way")
        if current is None or score > current.score:
            result[equipment_id] = entry
        elif current.equipment_name is None and equipment_name:
            result[equipment_id] = _EquipmentEntry(
                score=current.score,
                equipment_name=equipment_name,
                source=current.source,
            )
    return result


async def _compute_1way(
    conn: AsyncConnection,
    prodclass_details: List[ProdclassDetail],
) -> Dict[int, _EquipmentEntry]:
    log.info(
        "equipment-selection: computing 1way scores for %s prodclasses",
        len(prodclass_details),
    )
    equipment: Dict[int, _EquipmentEntry] = {}
    for detail in prodclass_details:
        log.debug(
            "equipment-selection: 1way processing prodclass %s with score %s",
            detail.prodclass_id,
            detail.score_1,
        )
        score_multiplier = detail.score_1
        # direct workshops
        workshops = await _fetch_workshops(conn, [detail.prodclass_id])
        if workshops:
            detail.path = "direct"
            detail.workshops_count = len(workshops)
            equip_rows = await _fetch_equipment_for_workshops(conn, [w["id"] for w in workshops])
            detail.equipment_count = len(equip_rows)
            log.debug(
                "equipment-selection: 1way direct path prodclass %s -> workshops=%s equipment=%s",
                detail.prodclass_id,
                detail.workshops_count,
                detail.equipment_count,
            )
            direct_scores = _calc_equipment_scores(equip_rows, score_multiplier)
            for equipment_id, entry in direct_scores.items():
                current = equipment.get(equipment_id)
                if current is None or entry.score > current.score:
                    equipment[equipment_id] = entry
                elif current.equipment_name is None and entry.equipment_name:
                    equipment[equipment_id] = _EquipmentEntry(
                        score=current.score,
                        equipment_name=entry.equipment_name,
                        source=current.source,
                    )
            continue

        # fallback to industry
        industry_id = await _fetch_industry_id(conn, detail.prodclass_id)
        detail.industry_id = industry_id
        if industry_id is None:
            detail.path = "no-industry"
            log.debug(
                "equipment-selection: 1way no industry for prodclass %s",
                detail.prodclass_id,
            )
            continue

        industry_prodclasses_sql = text(
            "SELECT id FROM ib_prodclass WHERE industry_id = :iid ORDER BY id"
        )
        rows = await conn.execute(industry_prodclasses_sql, {"iid": industry_id})
        prodclass_ids = [int(r[0]) for r in rows]
        if not prodclass_ids:
            detail.path = "industry-no-prodclass"
            log.debug(
                "equipment-selection: 1way industry %s has no prodclasses for prodclass %s",
                industry_id,
                detail.prodclass_id,
            )
            continue

        workshops = await _fetch_workshops(conn, prodclass_ids)
        if not workshops:
            detail.path = "industry-no-workshops"
            log.debug(
                "equipment-selection: 1way industry %s has no workshops for prodclass %s",
                industry_id,
                detail.prodclass_id,
            )
            continue

        detail.path = "industry"
        detail.workshops_count = len(workshops)
        equip_rows = await _fetch_equipment_for_workshops(conn, [w["id"] for w in workshops])
        detail.equipment_count = len(equip_rows)
        log.debug(
            "equipment-selection: 1way industry path prodclass %s -> industry %s workshops=%s equipment=%s",
            detail.prodclass_id,
            industry_id,
            detail.workshops_count,
            detail.equipment_count,
        )
        multiplier = score_multiplier * 0.75
        industry_scores = _calc_equipment_scores(equip_rows, multiplier)
        for equipment_id, entry in industry_scores.items():
            current = equipment.get(equipment_id)
            if current is None or entry.score > current.score:
                equipment[equipment_id] = entry
            elif current.equipment_name is None and entry.equipment_name:
                equipment[equipment_id] = _EquipmentEntry(
                    score=current.score,
                    equipment_name=entry.equipment_name,
                    source=current.source,
                )

    log.info(
        "equipment-selection: computed 1way scores for %s equipment ids",
        len(equipment),
    )
    return equipment


def _merge_goods_type_scores(goods_types: Sequence[GoodsTypeItem]) -> Dict[int, float]:
    scores: Dict[int, float] = {}
    for item in goods_types:
        if item.goods_type_id is None:
            continue
        score = item.goods_types_score or 0.0
        prev = scores.get(item.goods_type_id, 0.0)
        if score > prev:
            scores[item.goods_type_id] = score
    return scores


async def _compute_2way(
    conn: AsyncConnection, goods_types: Sequence[GoodsTypeItem]
) -> Dict[int, _EquipmentEntry]:
    goods_scores = _merge_goods_type_scores(goods_types)
    if not goods_scores:
        log.info("equipment-selection: skipping 2way computation — no goods type scores")
        return {}
    log.info(
        "equipment-selection: computing 2way scores for goods type ids %s",
        list(goods_scores.keys()),
    )

    sql = (
        text(
            """
            SELECT
                ieg.equipment_id AS id,
                e.equipment_name,
                e.equipment_score,
                g.goods_type_id
            FROM ib_equipment_goods AS ieg
            JOIN ib_goods AS g ON g.id = ieg.goods_id
            JOIN ib_equipment AS e ON e.id = ieg.equipment_id
            WHERE g.goods_type_id IN :gt_list
            """
        ).bindparams(bindparam("gt_list", expanding=True))
    )
    rows = await conn.execute(sql, {"gt_list": list(goods_scores.keys())})

    scores: Dict[int, _EquipmentEntry] = {}
    for row in rows:
        data = _row_to_dict(row)
        goods_type_id = data.get("goods_type_id")
        equipment_id = int(data["id"])
        base_score = _as_float(data.get("equipment_score")) or 0.0
        goods_score = goods_scores.get(goods_type_id, 0.0)
        score = round(base_score * goods_score, 4)
        if score <= 0:
            continue
        entry = _EquipmentEntry(
            score=score,
            equipment_name=data.get("equipment_name"),
            source="2way",
        )
        current = scores.get(equipment_id)
        if current is None or score > current.score:
            scores[equipment_id] = entry
    log.info(
        "equipment-selection: computed 2way scores for %s equipment ids",
        len(scores),
    )
    return scores


async def _compute_3way(
    conn: AsyncConnection, site_equipment: Sequence[SiteEquipmentItem]
) -> Dict[int, _EquipmentEntry]:
    log.info(
        "equipment-selection: computing 3way scores for %s site equipment items",
        len(site_equipment),
    )
    equipment_scores: Dict[int, _EquipmentEntry] = {}
    equipment_ids: List[int] = []
    for item in site_equipment:
        if item.equipment_id is None:
            continue
        equipment_ids.append(int(item.equipment_id))
        score = item.equipment_score or 0.0
        entry = _EquipmentEntry(
            score=round(score, 4),
            equipment_name=None,
            source="3way",
        )
        current = equipment_scores.get(item.equipment_id)
        if current is None or entry.score > current.score:
            equipment_scores[item.equipment_id] = entry

    if not equipment_scores:
        log.info("equipment-selection: skipping 3way name resolution — no scores computed")
        return {}

    sql = (
        text("SELECT id, equipment_name FROM ib_equipment WHERE id IN :ids").bindparams(
            bindparam("ids", expanding=True)
        )
    )
    rows = await conn.execute(sql, {"ids": sorted(set(equipment_ids))})
    names = {int(row[0]): row[1] for row in rows}

    for equipment_id, entry in list(equipment_scores.items()):
        name = names.get(int(equipment_id))
        equipment_scores[int(equipment_id)] = _EquipmentEntry(
            score=entry.score,
            equipment_name=name,
            source="3way",
        )
    log.info(
        "equipment-selection: computed 3way scores for %s equipment ids",
        len(equipment_scores),
    )
    return equipment_scores


def _merge_equipment_lists(
    *sources: Dict[int, _EquipmentEntry],
) -> List[EquipmentScore]:
    log.info(
        "equipment-selection: merging equipment lists from sources %s",
        [len(src) for src in sources],
    )
    priority = {"1way": 1, "2way": 2, "3way": 3}
    merged: Dict[int, _EquipmentEntry] = {}
    for source in sources:
        for equipment_id, entry in source.items():
            current = merged.get(equipment_id)
            if current is None:
                merged[equipment_id] = entry
                continue
            if entry.score > current.score or (
                entry.score == current.score
                and priority.get(entry.source, 99) < priority.get(current.source, 99)
            ):
                name = entry.equipment_name or current.equipment_name
                merged[equipment_id] = _EquipmentEntry(
                    score=entry.score,
                    equipment_name=name,
                    source=entry.source,
                )
            elif current.equipment_name is None and entry.equipment_name:
                merged[equipment_id] = _EquipmentEntry(
                    score=current.score,
                    equipment_name=entry.equipment_name,
                    source=current.source,
                )

    items = [
        EquipmentScore(
            id=equipment_id,
            equipment_name=entry.equipment_name,
            score=entry.score,
            source=entry.source,
        )
        for equipment_id, entry in merged.items()
    ]
    items.sort(key=lambda item: (-item.score, priority.get(item.source or "", 99), item.id))
    log.info(
        "equipment-selection: merged equipment list size=%s", len(items)
    )
    log.debug(
        "equipment-selection: merged equipment payload %s",
        [item.model_dump() for item in items],
    )
    return items


async def compute_equipment_selection(
    conn: AsyncConnection, client_request_id: int
) -> EquipmentSelectionResult:
    log.info(
        "equipment-selection: starting computation for clients_requests.id=%s",
        client_request_id,
    )
    client = await _fetch_client(conn, client_request_id)
    goods_types = await _fetch_goods_types(conn, client_request_id)
    site_equipment = await _fetch_site_equipment(conn, client_request_id)
    prodclass_rows = await _fetch_prodclass_rows(conn, client_request_id)

    prodclass_details = _aggregate_prodclass(prodclass_rows)
    equipment_1way_map = await _compute_1way(conn, prodclass_details)
    equipment_2way_map = await _compute_2way(conn, goods_types)
    equipment_3way_map = await _compute_3way(conn, site_equipment)

    def _to_list(data: Dict[int, _EquipmentEntry]) -> List[EquipmentScore]:
        items = [
            EquipmentScore(
                id=equipment_id,
                equipment_name=entry.equipment_name,
                score=entry.score,
                source=entry.source,
            )
            for equipment_id, entry in data.items()
        ]
        items.sort(key=lambda item: (-item.score, item.id))
        return items

    equipment_all = _merge_equipment_lists(
        equipment_1way_map,
        equipment_2way_map,
        equipment_3way_map,
    )

    result = EquipmentSelectionResult(
        client=client,
        goods_types=goods_types,
        site_equipment=site_equipment,
        prodclass_rows=prodclass_rows,
        prodclass_details=prodclass_details,
        equipment_1way=_to_list(equipment_1way_map),
        equipment_2way=_to_list(equipment_2way_map),
        equipment_3way=_to_list(equipment_3way_map),
        equipment_all=equipment_all,
    )
    log.info(
        "equipment-selection: finished computation for clients_requests.id=%s",
        client_request_id,
    )
    log.debug("equipment-selection: final payload %s", result.model_dump())
    return result


async def resolve_client_request_id(
    conn: AsyncConnection, inn: str
) -> Optional[int]:
    log.info("equipment-selection: resolving latest clients_requests id for INN %s", inn)
    sql = text(
        """
        SELECT id
        FROM public.clients_requests
        WHERE inn = :inn
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = (await conn.execute(sql, {"inn": inn.strip()})).first()
    if row is None:
        log.info(
            "equipment-selection: no clients_requests entries found for INN %s", inn
        )
        return None
    resolved = int(row[0])
    log.info(
        "equipment-selection: resolved INN %s to clients_requests.id=%s", inn, resolved
    )
    return resolved

