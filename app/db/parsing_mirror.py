# app/db/parsing_mirror.py
from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.db.postgres import get_postgres_engine
from app.db.parsing import _normalize_domain, _ensure_www, _okved_text

log = logging.getLogger("db.parsing_mirror")


def _pg_engine() -> Optional[AsyncEngine]:
    return get_postgres_engine()


# ---------------------------
# clients_requests helpers
# ---------------------------

async def get_last_domain_by_inn_pg(inn: str) -> Optional[str]:
    """
    Возвращает последний clients_requests.domain_1 для заданного ИНН из основной БД (POSTGRES).
    - Пропускаем NULL/пустые домены.
    - На выходе гарантируем 'www.' (через _ensure_www).
    """
    eng = _pg_engine()
    if eng is None:
        return None

    sql = text(
        """
        SELECT domain_1
        FROM public.clients_requests
        WHERE inn = :inn
          AND domain_1 IS NOT NULL
          AND TRIM(domain_1) <> ''
        ORDER BY id DESC
        LIMIT 1
        """
    )
    async with eng.begin() as conn:
        row = (await conn.execute(sql, {"inn": inn})).first()
        if not row:
            log.info("PG: last domain not found for inn=%s", inn)
            return None
        dom = row[0]
        try:
            ensured = _ensure_www(dom)
            return ensured
        except Exception:
            return str(dom) if dom is not None else None


async def get_domains_by_inn_pg(inn: str) -> list[str]:
    """Возвращает все clients_requests.domain_1 для заданного ИНН из основной БД."""

    eng = _pg_engine()
    if eng is None:
        return []

    sql = text(
        """
        SELECT domain_1, domain_2
        FROM public.clients_requests
        WHERE inn = :inn
        ORDER BY id DESC
        """
    )

    async with eng.begin() as conn:
        rows = await conn.execute(sql, {"inn": inn})
        values: list[str] = []
        for row in rows:
            if not row:
                continue
            for value in row:
                if value is None:
                    continue
                text = str(value).strip()
                if text:
                    values.append(text)
        return values


_IB_CLIENTS_SITE_COLUMNS: Optional[list[str]] = None


async def _get_ib_clients_site_columns() -> list[str]:
    """Определяет список колонок с сайтами в public.ib_clients."""

    global _IB_CLIENTS_SITE_COLUMNS
    if _IB_CLIENTS_SITE_COLUMNS is not None:
        return _IB_CLIENTS_SITE_COLUMNS

    eng = _pg_engine()
    if eng is None:
        _IB_CLIENTS_SITE_COLUMNS = []
        return []

    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'ib_clients'
          AND column_name ILIKE '%site%'
        ORDER BY ordinal_position
        """
    )

    async with eng.begin() as conn:
        res = await conn.execute(sql)
        cols = [str(row[0]) for row in res if row and row[0]]

    _IB_CLIENTS_SITE_COLUMNS = cols
    return cols


async def get_ib_clients_domains_pg(inn: str) -> list[str]:
    """Возвращает значения колонок с сайтами из public.ib_clients для заданного ИНН."""

    eng = _pg_engine()
    if eng is None:
        return []

    columns = await _get_ib_clients_site_columns()
    if not columns:
        return []

    cols_sql = ", ".join(f'"{col}"' for col in columns)
    sql = text(f"SELECT {cols_sql} FROM public.ib_clients WHERE inn = :inn")

    async with eng.begin() as conn:
        res = await conn.execute(sql, {"inn": inn})
        rows = res.fetchall()

    values: list[str] = []
    for row in rows:
        for value in row:
            if value is None:
                continue
            values.append(str(value))

    return values


async def get_clients_request_id_pg(inn: str, domain_1: Optional[str] = None) -> Optional[int]:
    """
    Возвращает id последней записи из POSTGRES.public.clients_requests по ИНН (+ опц. domain_1).
    По соглашению domain_1 в clients_requests хранится С 'www.'.
    """
    eng = _pg_engine()
    if eng is None:
        return None

    if domain_1:
        sql = text(
            "SELECT id FROM public.clients_requests "
            "WHERE inn = :inn AND domain_1 = :d ORDER BY id DESC LIMIT 1"
        )
        params = {"inn": inn, "d": _ensure_www(domain_1)}
    else:
        sql = text(
            "SELECT id FROM public.clients_requests "
            "WHERE inn = :inn ORDER BY id DESC LIMIT 1"
        )
        params = {"inn": inn}

    async with eng.begin() as conn:
        row = (await conn.execute(sql, params)).first()
        return int(row[0]) if row else None


async def push_clients_request_pg(
    summary: dict,
    domain: Optional[str] = None,
    domain_secondary: Optional[str] = None,
) -> bool:
    """
    Upsert в POSTGRES.public.clients_requests по inn.
    По соглашению domain_1 пишем С 'www.'.
    """
    eng = _pg_engine()
    if eng is None:
        return False

    # подготовка ряда
    main_okved = summary.get("main_okved")
    okveds = summary.get("okveds") or []

    def _get_code(x):
        if isinstance(x, dict):
            return x.get("code") or x.get("value") or x.get("okved")
        return str(x) if x is not None else None

    secondaries: list[str] = []
    for it in okveds:
        code = _get_code(it)
        if main_okved and code and str(code) == str(main_okved):
            continue
        txt = _okved_text(it)
        if txt:
            secondaries.append(txt)
    secondaries = secondaries[:7]

    def pick(i: int) -> Optional[str]:
        return secondaries[i] if i < len(secondaries) else None

    row = {
        "company_name": summary.get("short_name"),
        "inn": summary.get("inn"),
        "domain_1": _ensure_www(domain),
        "domain_2": _ensure_www(domain_secondary),
        "okved_main": main_okved,
        "okved_vtor_1": pick(0),
        "okved_vtor_2": pick(1),
        "okved_vtor_3": pick(2),
        "okved_vtor_4": pick(3),
        "okved_vtor_5": pick(4),
        "okved_vtor_6": pick(5),
        "okved_vtor_7": pick(6),
    }

    if not row.get("inn"):
        log.info("PG: Не указан ИНН — запись в clients_requests пропущена.")
        return False

    sql_update = text("""
        UPDATE public.clients_requests
        SET company_name=:company_name,
            domain_1=COALESCE(:domain_1, domain_1),
            domain_2=:domain_2,
            okved_main=:okved_main,
            okved_vtor_1=:okved_vtor_1,
            okved_vtor_2=:okved_vtor_2,
            okved_vtor_3=:okved_vtor_3,
            okved_vtor_4=:okved_vtor_4,
            okved_vtor_5=:okved_vtor_5,
            okved_vtor_6=:okved_vtor_6,
            okved_vtor_7=:okved_vtor_7
        WHERE inn=:inn
    """)

    sql_insert = text("""
        INSERT INTO public.clients_requests
        (company_name, inn, domain_1, domain_2, okved_main,
         okved_vtor_1, okved_vtor_2, okved_vtor_3, okved_vtor_4, okved_vtor_5, okved_vtor_6, okved_vtor_7)
        VALUES
        (:company_name, :inn, :domain_1, :domain_2, :okved_main,
         :okved_vtor_1, :okved_vtor_2, :okved_vtor_3, :okved_vtor_4, :okved_vtor_5, :okved_vtor_6, :okved_vtor_7)
    """)

    try:
        async with eng.begin() as conn:
            res = await conn.execute(sql_update, row)
            if getattr(res, "rowcount", 0) == 0:
                await conn.execute(sql_insert, row)
        log.info("PG clients_requests upsert: inn=%s, domain_1=%s", row["inn"], row["domain_1"])
        return True
    except Exception as e:
        log.warning("PG clients_requests upsert не удался (inn=%s): %s", row.get("inn"), e)
        return False


# ---------------------------
# pars_site insert (основная БД)
# ---------------------------

# Кэш колонок pars_site основной БД
_PARS_SITE_COLUMNS: Optional[set[str]] = None

async def _get_pars_site_columns() -> set[str]:
    """Читает список колонок public.pars_site (основная БД) и кэширует его."""
    global _PARS_SITE_COLUMNS
    if _PARS_SITE_COLUMNS is not None:
        return _PARS_SITE_COLUMNS

    eng = _pg_engine()
    cols: set[str] = set()
    if eng is None:
        _PARS_SITE_COLUMNS = cols
        return cols

    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'pars_site'
    """)
    try:
        async with eng.begin() as conn:
            rows = await conn.execute(sql)
            for r in rows:
                cols.add(str(r[0]))
    except Exception as e:
        log.warning("PG: не удалось прочитать колонки pars_site: %s", e)

    _PARS_SITE_COLUMNS = cols
    log.info("PG pars_site columns detected: %s", sorted(cols))
    return cols


def _coerce_chunk(ch: Mapping[str, Any] | tuple[int, int, str]) -> Optional[str]:
    """
    Приводим внешний чанк к строке-тексту для записи в БД.
    Вход: либо кортеж (start,end,text), либо маппинг {text: "..."}.
    """
    if isinstance(ch, tuple) and len(ch) == 3:
        _s, _e, txt = ch
    elif isinstance(ch, Mapping):
        txt = ch.get("text")
    else:
        return None
    if txt is None:
        return None
    t = str(txt)
    return t if t else None


async def pars_site_insert_chunks_pg(
    *,
    company_id: int,
    domain_1: str,
    url: str,
    chunks: Iterable[Mapping[str, Any] | tuple[int, int, str]],
    batch_size: int = 500,
) -> int:
    """
    Массовая вставка чанков в ОСНОВНУЮ БД POSTGRES.public.pars_site.

    Схема основной БД из твоего дампа:
      id, company_id, domain_1, text_par, url, created_at, text_vector, description

    => Пишем в text_par (если он есть), иначе в description.
       Колонок start/end/idx нет — дедуп по (company_id, domain_1, url, <text_col>).
       domain_1 сохраняем БЕЗ 'www.'.
    """
    eng = _pg_engine()
    if eng is None:
        return 0

    cols = await _get_pars_site_columns()
    # определяем колонку для текста
    text_col: Optional[str] = None
    if "text_par" in cols:
        text_col = "text_par"
    elif "description" in cols:
        text_col = "description"

    if not text_col:
        msg = (
            "В основной БД public.pars_site нет подходящей текстовой колонки "
            "(ожидались text_par или description). Обнаружено: "
            + ", ".join(sorted(cols))
        )
        log.error(msg)
        raise RuntimeError(msg)

    dom = _normalize_domain(domain_1) or domain_1
    inserted = 0

    # Готовим SQL под выбранный text_col
    sql = text(f"""
        INSERT INTO public.pars_site (company_id, domain_1, url, {text_col})
        SELECT :company_id, :domain_1, :url, :text
        WHERE NOT EXISTS (
            SELECT 1 FROM public.pars_site ps
            WHERE ps.company_id = :company_id
              AND ps.domain_1 = :domain_1
              AND ps.url = :url
              AND ps.{text_col} = :text
        )
    """)

    # Батч-вставка
    buf: list[dict] = []
    async with eng.begin() as conn:
        for ch in chunks:
            t = _coerce_chunk(ch)
            if not t:
                continue
            buf.append({"company_id": company_id, "domain_1": dom, "url": url, "text": t})
            if len(buf) >= batch_size:
                for row in buf:
                    res = await conn.execute(sql, row)
                    inserted += int(getattr(res, "rowcount", 0) or 0)
                buf.clear()
        if buf:
            for row in buf:
                res = await conn.execute(sql, row)
                inserted += int(getattr(res, "rowcount", 0) or 0)
            buf.clear()

    return inserted
