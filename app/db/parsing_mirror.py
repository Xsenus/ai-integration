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


async def push_clients_request_pg(summary: dict, domain: Optional[str] = None) -> bool:
    """
    Upsert в POSTGRES.public.clients_requests по inn.
    По соглашению domain_1 пишем С 'www.'.
    """
    eng = _pg_engine()
    if eng is None:
        return False

    # подготовка ряда — повторяем логику из app.db.parsing
    emails = summary.get("emails") or []
    if isinstance(emails, list):
        emails_str = ", ".join(str(e) for e in emails if e)
    else:
        emails_str = str(emails) if emails else None

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
        "domain_2": emails_str,
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
            okved_vtor_4=:окved_vтор_4,
            okved_vtor_5=:okved_vtor_5,
            okved_vtor_6=:okved_vtor_6,
            okved_vtor_7=:okved_vtor_7
        WHERE inn=:inn
    """.replace("окved_vтор_4", "okved_vtor_4"))  # защита от раскладки

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


async def pars_site_insert_chunks_pg(
    *,
    company_id: int,
    domain_1: str,
    url: str,
    chunks: Iterable[Mapping[str, Any] | tuple[int, int, str]],
    batch_size: int = 500,
) -> int:
    """
    Массовая вставка чанков в POSTGRES.public.pars_site.
    По соглашению pars_site.domain_1 — без 'www.'.
    """
    eng = _pg_engine()
    if eng is None:
        return 0

    dom = _normalize_domain(domain_1) or domain_1
    inserted = 0

    def _coerce(ch: Mapping[str, Any] | tuple[int, int, str]) -> Optional[dict]:
        if isinstance(ch, tuple) and len(ch) == 3:
            start, end, txt = ch
        elif isinstance(ch, Mapping):
            start = int(ch.get("start", 0))
            end = int(ch.get("end", 0))
            txt = ch.get("text")
        else:
            return None
        if txt is None:
            return None
        s = int(start)
        e = int(end)
        t = str(txt)
        if not t:
            return None
        return {"company_id": company_id, "domain_1": dom, "url": url, "start": s, "end": e, "text": t}

    sql = text("""
        INSERT INTO public.pars_site (company_id, domain_1, url, start, "end", text)
        SELECT :company_id, :domain_1, :url, :start, :end, :text
        WHERE NOT EXISTS (
            SELECT 1 FROM public.pars_site ps
            WHERE ps.company_id = :company_id
              AND ps.domain_1 = :domain_1
              AND ps.url = :url
              AND ps.start = :start
              AND ps."end" = :end
        )
    """)

    buf: list[dict] = []
    async with eng.begin() as conn:
        for ch in chunks:
            row = _coerce(ch)
            if not row:
                continue
            buf.append(row)
            if len(buf) >= batch_size:
                for r in buf:
                    res = await conn.execute(sql, r)
                    inserted += int(getattr(res, "rowcount", 0) or 0)
                buf.clear()
        if buf:
            for r in buf:
                res = await conn.execute(sql, r)
                inserted += int(getattr(res, "rowcount", 0) or 0)
            buf.clear()

    return inserted
