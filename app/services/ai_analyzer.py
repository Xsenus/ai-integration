from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.engine import Result

from app.db.bitrix import get_bitrix_engine          # ← Bitrix сначала
from app.db.postgres import get_postgres_engine

# ---------------------- helpers ----------------------

_DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9\-\.]*\.[a-z]{2,}$", re.IGNORECASE)

_GOODS_LOOKUP_TABLES = ("goods_types", "ib_goods_types", "goods_type")
_EQUIPMENT_LOOKUP_TABLES = ("equipment", "ib_equipment", "equipment_types")
_PRODCLASS_LOOKUP_TABLES = ("ib_prodclass", "prodclass", "product_class")

_PREFERRED_NAME_COLUMNS = (
    "name",
    "title",
    "full_name",
    "short_name",
    "label",
)
_GROUP_NAME_COLUMNS = (
    "group_name",
    "section_name",
    "category_name",
    "class_name",
)

_MAX_PRODUCTS = 100
_MAX_EQUIPMENT = 100

def _normalize_site(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = value.strip()
    s = re.split(r"[\s,;]+", s)[0]
    if s.startswith("http://") or s.startswith("https://"):
        s = s.rstrip("/")
        m = re.match(r"^https?://([^/\s]+)", s, flags=re.I)
        host = (m.group(1).lower() if m else "").strip()
        return f"https://{host}" if host else None
    s = s.lower()
    if _DOMAIN_RE.match(s):
        return f"https://{s}"
    return None

def _okved_to_industry(okved: Optional[str]) -> Optional[str]:
    if not okved:
        return None
    m = re.search(r"(\d{2})", okved)
    if not m:
        return None
    code2 = int(m.group(1))
    if 10 <= code2 <= 12:  return "Пищевая промышленность"
    if 13 <= code2 <= 15:  return "Текстиль/одежда"
    if 16 <= code2 <= 18:  return "Деревообработка/бумага/печать"
    if 19 <= code2 <= 22:  return "Химия/полимеры"
    if 23 <= code2 <= 25:  return "Неметаллы/металлы/машиностроение"
    if 26 <= code2 <= 28:  return "Электроника/оборудование"
    if 29 <= code2 <= 30:  return "Авто/транспортное машиностроение"
    if 31 <= code2 <= 33:  return "Прочее производство/ремонт"
    if 35 <= code2 <= 39:  return "Энергетика/вода/утилизация"
    if 41 <= code2 <= 43:  return "Строительство"
    if 45 <= code2 <= 47:  return "Торговля"
    if 49 <= code2 <= 53:  return "Транспорт и логистика"
    if 55 <= code2 <= 56:  return "Гостиницы/общепит"
    if 58 <= code2 <= 63:  return "IT/связь/медиа"
    if 64 <= code2 <= 66:  return "Финансы/страхование"
    if 68 == code2:       return "Недвижимость"
    if 69 <= code2 <= 75: return "Проф. и научные услуги"
    if 77 <= code2 <= 82: return "Адм. услуги"
    if 84 == code2:       return "Госуправление"
    if 85 == code2:       return "Образование"
    if 86 <= code2 <= 88: return "Здравоохранение/соцуслуги"
    if 90 <= code2 <= 93: return "Культура/спорт/развлечения"
    if 94 <= code2 <= 96: return "Общественные/личные услуги"
    return None

async def _table_exists(conn, schema: str, table: str) -> bool:
    q = text("""
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = :schema AND table_name = :table
        )
    """)
    res = await conn.execute(q, {"schema": schema, "table": table})
    return bool(res.scalar())

async def _get_table_columns(conn, schema: str, table: str) -> set[str]:
    """Возвращает множество колонок таблицы в нижнем регистре."""

    q = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        """
    )
    res = await conn.execute(q, {"schema": schema, "table": table})
    return {str(row[0]).lower() for row in res}


async def _load_lookup_table(
    conn,
    table_candidates: Iterable[str],
    id_candidates: Iterable[str],
) -> dict[Any, dict[str, Any]]:
    """Пытаемся прочитать справочник по первой доступной таблице."""

    for table in table_candidates:
        cols = await _get_table_columns(conn, "public", table)
        if not cols:
            continue
        id_col = next((col for col in id_candidates if col in cols), None)
        if not id_col:
            continue
        select_cols: list[str] = [id_col]
        for col in (*_PREFERRED_NAME_COLUMNS, *_GROUP_NAME_COLUMNS, "code"):
            if col in cols and col not in select_cols:
                select_cols.append(col)
        try:
            sql = text(f"SELECT {', '.join(select_cols)} FROM public.{table}")
            rows = await conn.execute(sql)
        except Exception:
            continue
        mapping: dict[Any, dict[str, Any]] = {}
        for row in rows.mappings():
            key = row.get(id_col)
            if key is None:
                continue
            mapping[key] = {col: row.get(col) for col in select_cols if col != id_col}
        if mapping:
            return mapping
    return {}


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _score_sort_key(row: Mapping[str, Any], score_key: str) -> tuple[float, int]:
    score = _as_float(row.get(score_key))
    primary = -score if score is not None else 1.0
    identifier = _safe_int(row.get("id")) or 0
    return primary, identifier


def _domain_from_value(value: Optional[str]) -> Optional[str]:
    normalized = _normalize_site(value)
    if not normalized:
        return None
    try:
        parsed = urlparse(normalized)
        host = (parsed.netloc or parsed.path or "").strip()
        return host or None
    except Exception:
        sanitized = normalized.replace("https://", "").replace("http://", "")
        return sanitized or None


def _extract_domain(row: Mapping[str, Any]) -> Optional[str]:
    for key in ("domain_1", "url", "domain"):
        value = row.get(key)
        if not value:
            continue
        domain = _domain_from_value(str(value))
        if domain:
            return domain
    return None


def _extract_url(row: Mapping[str, Any]) -> Optional[str]:
    raw = row.get("url")
    if raw:
        s = str(raw).strip()
        if s:
            return s
    domain = row.get("domain_1") or row.get("domain")
    normalized = _normalize_site(str(domain)) if domain else None
    return normalized


def _resolve_from_lookup(
    lookup: Mapping[Any, Mapping[str, Any]] | None,
    identifier: Any,
    *,
    prefer_group: bool = False,
) -> Optional[str]:
    if not lookup or identifier is None:
        return None
    info = lookup.get(identifier)
    if not info:
        return None
    columns = _GROUP_NAME_COLUMNS if prefer_group else _PREFERRED_NAME_COLUMNS
    for col in columns:
        value = info.get(col)
        if value:
            return str(value).strip()
    # если искали group, попробуем имя, и наоборот
    fallback_columns = _PREFERRED_NAME_COLUMNS if prefer_group else _GROUP_NAME_COLUMNS
    for col in fallback_columns:
        value = info.get(col)
        if value:
            return str(value).strip()
    value = info.get("code")
    return str(value).strip() if value else None


def _resolve_goods_group(
    row: Mapping[str, Any],
    lookup: Mapping[Any, Mapping[str, Any]] | None,
) -> Optional[str]:
    goods_id = row.get("goods_type_id")
    label = _resolve_from_lookup(lookup, goods_id, prefer_group=True)
    if not label:
        label = str(row.get("goods_type") or "").strip() or None
    if label and goods_id is not None and str(goods_id) not in label:
        return f"{label} (ID {goods_id})"
    if label:
        return label
    if goods_id is not None:
        return f"ID {goods_id}"
    return None


def _resolve_prodclass_name(
    row: Mapping[str, Any],
    lookup: Mapping[Any, Mapping[str, Any]] | None,
) -> tuple[Optional[str], bool]:
    for key in ("prodclass_name", "prodclass_title", "prodclass_text"):
        value = row.get(key)
        if value:
            return str(value).strip(), False
    identifier = row.get("prodclass")
    label = _resolve_from_lookup(lookup, identifier)
    if label:
        return label, False
    if identifier is None:
        return None, True
    return f"Prodclass {identifier}", True


def _compose_products(
    prod_rows: Iterable[Mapping[str, Any]],
    goods_rows: Iterable[Mapping[str, Any]],
    prod_lookup: Mapping[Any, Mapping[str, Any]] | None,
    goods_lookup: Mapping[Any, Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    goods_by_text: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in goods_rows:
        text_id = _safe_int(row.get("text_par_id"))
        if text_id is None:
            continue
        goods_by_text[text_id].append(row)
    for rows in goods_by_text.values():
        rows.sort(key=lambda r: _score_sort_key(r, "goods_types_score"))

    results: list[dict[str, Any]] = []
    seen: set[tuple[str, Optional[str]]] = set()
    used_goods_ids: set[Any] = set()

    for row in sorted(prod_rows, key=lambda r: _score_sort_key(r, "prodclass_score")):
        name, is_placeholder = _resolve_prodclass_name(row, prod_lookup)
        text_id = _safe_int(row.get("text_pars_id"))
        goods_entry = None
        goods_group = None
        if text_id is not None:
            for candidate in goods_by_text.get(text_id, []):
                candidate_id = candidate.get("id")
                if candidate_id in used_goods_ids:
                    continue
                goods_entry = candidate
                goods_group = _resolve_goods_group(candidate, goods_lookup)
                used_goods_ids.add(candidate_id)
                break
        if is_placeholder and goods_entry:
            alt = str(goods_entry.get("goods_type") or "").strip()
            if alt:
                name = alt
                is_placeholder = False
        if not name:
            continue
        domain = _extract_domain(row) or (goods_entry and _extract_domain(goods_entry))
        url = _extract_url(row) or (goods_entry and _extract_url(goods_entry))
        key = (name, url or domain)
        if key in seen:
            continue
        results.append(
            {
                "name": name,
                "goods_group": goods_group,
                "domain": domain,
                "url": url,
            }
        )
        seen.add(key)

    # добавим остатки goods_types, для которых не было prodclass
    for rows in goods_by_text.values():
        for row in rows:
            if row.get("id") in used_goods_ids:
                continue
            name = str(row.get("goods_type") or "").strip()
            if not name:
                continue
            domain = _extract_domain(row)
            url = _extract_url(row)
            key = (name, url or domain)
            if key in seen:
                continue
            results.append(
                {
                    "name": name,
                    "goods_group": _resolve_goods_group(row, goods_lookup),
                    "domain": domain,
                    "url": url,
                }
            )
            seen.add(key)

    return results


def _resolve_equipment_name(
    row: Mapping[str, Any],
    lookup: Mapping[Any, Mapping[str, Any]] | None,
) -> Optional[str]:
    for key in ("equipment_name", "equipment_title"):
        value = row.get(key)
        if value:
            return str(value).strip()
    identifier = row.get("equipment_id")
    label = _resolve_from_lookup(lookup, identifier)
    if label:
        return label
    value = row.get("equipment")
    if value:
        return str(value).strip()
    if identifier is not None:
        return f"Equipment {identifier}"
    return None


def _resolve_equipment_group(
    row: Mapping[str, Any],
    lookup: Mapping[Any, Mapping[str, Any]] | None,
) -> Optional[str]:
    identifier = row.get("equipment_id")
    label = _resolve_from_lookup(lookup, identifier, prefer_group=True)
    if label:
        return label
    if identifier is not None:
        return f"ID {identifier}"
    return None


def _compose_equipment(
    equipment_rows: Iterable[Mapping[str, Any]],
    lookup: Mapping[Any, Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[tuple[str, Optional[str]]] = set()
    for row in sorted(equipment_rows, key=lambda r: _score_sort_key(r, "equipment_score")):
        name = _resolve_equipment_name(row, lookup)
        if not name:
            continue
        domain = _extract_domain(row)
        url = _extract_url(row)
        key = (name, url or domain)
        if key in seen:
            continue
        results.append(
            {
                "name": name,
                "equip_group": _resolve_equipment_group(row, lookup),
                "domain": domain,
                "url": url,
            }
        )
        seen.add(key)
    return results

# ---------------------- core ----------------------

async def analyze_company_by_inn(inn: str) -> dict:
    """
    Источники:
      - Postgres: public.clients_requests  (домены/UTP/письмо/okved_main)
      - Bitrix:   public.dadata_result     (fallback main_okved — СНАЧАЛА здесь)
      - Postgres: public.dadata_result     (fallback №2 — если в Bitrix нет таблицы/строки)
    """
    eng_pg = get_postgres_engine()
    eng_bx = get_bitrix_engine()

    if eng_pg is None and eng_bx is None:
        return {
            "inn": inn, "domain1": None, "domain2": None, "industry": None,
            "sites": [], "products": [], "equipment": [],
            "utp": None, "letter": None, "note": "no databases configured",
        }

    # ---- 1) clients_requests (PG) ----
    cr = None
    company_id: Optional[int] = None
    pars_rows: list[dict[str, Any]] = []
    goods_rows: list[dict[str, Any]] = []
    prod_rows: list[dict[str, Any]] = []
    equipment_rows: list[dict[str, Any]] = []
    goods_lookup: dict[Any, dict[str, Any]] = {}
    prod_lookup: dict[Any, dict[str, Any]] = {}
    equipment_lookup: dict[Any, dict[str, Any]] = {}

    if eng_pg is not None:
        sql_cr = text("""
            SELECT id, domain_1, domain_2, okved_main, utp, pismo,
                   site_1_description, site_2_description
            FROM public.clients_requests
            WHERE inn = :inn
            ORDER BY COALESCE(ended_at, created_at) DESC NULLS LAST, id DESC
            LIMIT 1
        """)
        async with eng_pg.connect() as conn:
            r1: Result = await conn.execute(sql_cr, {"inn": inn})
            cr = r1.mappings().first()
        if cr and cr.get("id") is not None:
            company_id = _safe_int(cr.get("id"))

    if eng_pg is not None and company_id:
        async with eng_pg.connect() as conn:
            if await _table_exists(conn, "public", "pars_site"):
                sql_pars = text(
                    """
                    SELECT id, domain_1, url, created_at
                    FROM public.pars_site
                    WHERE company_id = :company_id
                    ORDER BY created_at DESC NULLS LAST, id DESC
                    """
                )
                res_pars = await conn.execute(sql_pars, {"company_id": company_id})
                pars_rows = [dict(row) for row in res_pars.mappings().all()]

            if await _table_exists(conn, "public", "ai_site_goods_types"):
                sql_goods = text(
                    """
                    SELECT gt.id, gt.text_par_id, gt.goods_type, gt.goods_type_id, gt.goods_types_score,
                           ps.domain_1, ps.url, ps.id AS pars_site_id
                    FROM public.ai_site_goods_types AS gt
                    JOIN public.pars_site AS ps ON ps.id = gt.text_par_id
                    WHERE ps.company_id = :company_id
                    ORDER BY gt.goods_types_score DESC NULLS LAST, gt.id DESC
                    """
                )
                res_goods = await conn.execute(sql_goods, {"company_id": company_id})
                goods_rows = [dict(row) for row in res_goods.mappings().all()]
                if goods_rows:
                    goods_lookup = await _load_lookup_table(
                        conn, _GOODS_LOOKUP_TABLES, ("id", "goods_type_id")
                    )

            if await _table_exists(conn, "public", "ai_site_prodclass"):
                sql_prod = text(
                    """
                    SELECT pc.id, pc.text_pars_id, pc.prodclass, pc.prodclass_score,
                           ps.domain_1, ps.url, ps.id AS pars_site_id
                    FROM public.ai_site_prodclass AS pc
                    JOIN public.pars_site AS ps ON ps.id = pc.text_pars_id
                    WHERE ps.company_id = :company_id
                    ORDER BY pc.prodclass_score DESC NULLS LAST, pc.id DESC
                    """
                )
                res_prod = await conn.execute(sql_prod, {"company_id": company_id})
                prod_rows = [dict(row) for row in res_prod.mappings().all()]
                if prod_rows:
                    prod_lookup = await _load_lookup_table(
                        conn, _PRODCLASS_LOOKUP_TABLES, ("id", "prodclass", "prodclass_id")
                    )

            if await _table_exists(conn, "public", "ai_site_equipment"):
                sql_equipment = text(
                    """
                    SELECT eq.id, eq.text_pars_id, eq.equipment, eq.equipment_id, eq.equipment_score,
                           ps.domain_1, ps.url, ps.id AS pars_site_id
                    FROM public.ai_site_equipment AS eq
                    JOIN public.pars_site AS ps ON ps.id = eq.text_pars_id
                    WHERE ps.company_id = :company_id
                    ORDER BY eq.equipment_score DESC NULLS LAST, eq.id DESC
                    """
                )
                res_eq = await conn.execute(sql_equipment, {"company_id": company_id})
                equipment_rows = [dict(row) for row in res_eq.mappings().all()]
                if equipment_rows:
                    equipment_lookup = await _load_lookup_table(
                        conn, _EQUIPMENT_LOOKUP_TABLES, ("id", "equipment_id")
                    )

    # --- домены из CR + pars_site ---
    urls: list[str] = []
    if cr:
        for raw in (cr.get("domain_1"), cr.get("domain_2")):
            u = _normalize_site(raw)
            if u and u not in urls:
                urls.append(u)
    for row in pars_rows:
        for raw in (row.get("url"), row.get("domain_1")):
            u = _normalize_site(str(raw)) if raw else None
            if u and u not in urls:
                urls.append(u)
    domain1 = urls[0] if len(urls) >= 1 else None
    domain2 = urls[1] if len(urls) >= 2 else None

    products = _compose_products(prod_rows, goods_rows, prod_lookup, goods_lookup)
    if len(products) > _MAX_PRODUCTS:
        products = products[:_MAX_PRODUCTS]

    equipment = _compose_equipment(equipment_rows, equipment_lookup)
    if len(equipment) > _MAX_EQUIPMENT:
        equipment = equipment[:_MAX_EQUIPMENT]

    # ---- 2) main_okved fallback: сначала Bitrix.dadata_result ----
    okved_fallback = None
    bx_used = False
    if eng_bx is not None:
        async with eng_bx.connect() as conn_bx:
            if await _table_exists(conn_bx, "public", "dadata_result"):
                sql_dd_bx = text("""
                    SELECT main_okved
                    FROM public.dadata_result
                    WHERE inn = :inn
                    LIMIT 1
                """)
                rbx: Result = await conn_bx.execute(sql_dd_bx, {"inn": inn})
                row_bx = rbx.mappings().first()
                if row_bx and row_bx.get("main_okved"):
                    okved_fallback = row_bx.get("main_okved")
                    bx_used = True

    # ---- 3) если в Bitrix не нашли — пробуем Postgres.dadata_result ----
    if okved_fallback is None and eng_pg is not None:
        async with eng_pg.connect() as conn_pg:
            if await _table_exists(conn_pg, "public", "dadata_result"):
                sql_dd_pg = text("""
                    SELECT main_okved
                    FROM public.dadata_result
                    WHERE inn = :inn
                    LIMIT 1
                """)
                rpg: Result = await conn_pg.execute(sql_dd_pg, {"inn": inn})
                row_pg = rpg.mappings().first()
                if row_pg and row_pg.get("main_okved"):
                    okved_fallback = row_pg.get("main_okved")

    # --- индустрия: CR.okved_main > fallback(main_okved) ---
    okved_src = (cr or {}).get("okved_main") if cr else None
    if not okved_src:
        okved_src = okved_fallback
    industry = _okved_to_industry(okved_src)

    utp = (cr or {}).get("utp") if cr else None
    letter = (cr or {}).get("pismo") if cr else None

    note_bits = []
    if cr:
        note_bits.append("clients_requests(PG)")
    if pars_rows:
        note_bits.append("pars_site(PG)")
    if goods_rows:
        note_bits.append("ai_site_goods_types(PG)")
    if prod_rows:
        note_bits.append("ai_site_prodclass(PG)")
    if equipment_rows:
        note_bits.append("ai_site_equipment(PG)")
    if bx_used:
        note_bits.append("dadata_result(Bitrix)")
    elif okved_fallback is not None:
        note_bits.append("dadata_result(PG)")
    note = "filled from: " + ", ".join(note_bits) if note_bits else "no sources found"

    return {
        "inn": inn,
        "domain1": domain1,
        "domain2": domain2,
        "industry": industry,
        "sites": [u for u in urls if u],
        "products": products,
        "equipment": equipment,
        "utp": utp,
        "letter": letter,
        "note": note,
    }
