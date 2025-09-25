from __future__ import annotations

import re
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Result

from app.db.bitrix import get_bitrix_engine          # ← Bitrix сначала
from app.db.postgres import get_postgres_engine

# ---------------------- helpers ----------------------

_DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9\-\.]*\.[a-z]{2,}$", re.IGNORECASE)

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

    # --- домены из CR ---
    urls: list[str] = []
    if cr:
        for raw in (cr.get("domain_1"), cr.get("domain_2")):
            u = _normalize_site(raw)
            if u and u not in urls:
                urls.append(u)
    domain1 = urls[0] if len(urls) >= 1 else None
    domain2 = urls[1] if len(urls) >= 2 else None

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
    if cr: note_bits.append("clients_requests(PG)")
    if bx_used: note_bits.append("dadata_result(Bitrix)")
    elif okved_fallback is not None: note_bits.append("dadata_result(PG)")
    note = "filled from: " + ", ".join(note_bits) if note_bits else "no sources found"

    return {
        "inn": inn,
        "domain1": domain1,
        "domain2": domain2,
        "industry": industry,
        "sites": [u for u in urls if u],
        "products": [],
        "equipment": [],
        "utp": utp,
        "letter": letter,
        "note": note,
    }
