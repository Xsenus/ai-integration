# app/db/parsing.py
from __future__ import annotations

import logging
from typing import Any, Optional, Iterable, Mapping
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.config import settings

log = logging.getLogger("db.parsing")

_engine_parsing: Optional[AsyncEngine] = None


# ---------- Engine ----------

def get_parsing_engine() -> Optional[AsyncEngine]:
    """
    Ленивая инициализация движка для БД parsing_data.
    Если DSN не задан — возвращает None (мягкое отключение).
    """
    global _engine_parsing
    url = settings.parsing_url
    if not url:
        log.warning("PARSING_DATABASE_URL не задан — соединение с parsing_data отключено")
        return None
    if _engine_parsing is None:
        _engine_parsing = create_async_engine(
            url, pool_pre_ping=True, future=True, echo=settings.ECHO_SQL
        )
    return _engine_parsing


# ---------- Helpers ----------

async def table_exists(table_qualified: str) -> bool:
    """Проверяет наличие таблицы (например, 'public.clients_requests') в parsing_data."""
    eng = get_parsing_engine()
    if eng is None:
        return False
    try:
        async with eng.connect() as conn:
            res = await conn.execute(text("SELECT to_regclass(:t)"), {"t": table_qualified})
            return res.scalar_one_or_none() is not None
    except Exception as e:  # noqa: BLE001
        log.info("Ошибка соединения с parsing_data (%s) — пропускаю проверку %s.", e, table_qualified)
        return False


async def clients_requests_exists() -> bool:
    """Есть ли таблица public.clients_requests в parsing_data."""
    return await table_exists("public.clients_requests")


def _okved_text(item: Any) -> str | None:
    """Формат 'код — наименование' из dict/str."""
    if item is None:
        return None
    if isinstance(item, dict):
        code = item.get("code") or item.get("value") or item.get("okved")
        name = item.get("name") or item.get("short") or item.get("title")
        if code and name:
            return f"{code} — {name}"
        if code:
            return str(code)
        return name or None
    s = str(item).strip()
    return s or None


def _normalize_domain(domain: Optional[str]) -> Optional[str]:
    """Нормализация домена: вырезаем протокол/путь, host в lower без www."""
    if not domain:
        return None
    s = domain.strip()
    if not s:
        return None
    parsed = urlparse(s if "://" in s else "http://" + s)
    host = (parsed.netloc or parsed.path).strip().lower().rstrip("/")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _prepare_row_from_summary(summary: dict, domain: Optional[str] = None) -> dict:
    """
    Готовит поля под INSERT/UPDATE в public.clients_requests:
      - company_name, inn
      - domain_1 (нормализованный домен), domain_2 (e-mail'ы через запятую)
      - okved_main
      - okved_vtor_1..7 (до 7 вторичных ОКВЭД, без дубликата главного)
    """
    # emails -> строка
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

    # Собираем вторичные коды в list[str] без None внутри и без дубликата главного
    secondaries: list[str] = []
    for it in okveds:
        code = _get_code(it)
        if main_okved and code and str(code) == str(main_okved):
            continue
        txt = _okved_text(it)  # использует 'код — наименование', если dict
        if txt:
            secondaries.append(txt)
    secondaries = secondaries[:7]  # максимум 7

    def pick(i: int) -> Optional[str]:
        return secondaries[i] if i < len(secondaries) else None

    return {
        "company_name": summary.get("short_name"),
        "inn": summary.get("inn"),
        "domain_1": _normalize_domain(domain),
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


# ---------- CRUD for clients_requests ----------

async def get_clients_request_id(inn: str, domain_1: Optional[str] = None) -> Optional[int]:
    """Возвращает id последней записи по ИНН (+ опц. domain_1) из public.clients_requests."""
    eng = get_parsing_engine()
    if eng is None:
        return None

    if domain_1:
        sql = text(
            "SELECT id FROM public.clients_requests "
            "WHERE inn = :inn AND domain_1 = :d ORDER BY id DESC LIMIT 1"
        )
        params = {"inn": inn, "d": _normalize_domain(domain_1)}
    else:
        sql = text(
            "SELECT id FROM public.clients_requests "
            "WHERE inn = :inn ORDER BY id DESC LIMIT 1"
        )
        params = {"inn": inn}

    async with eng.begin() as conn:
        row = (await conn.execute(sql, params)).first()
        return int(row[0]) if row else None


async def push_clients_request(summary: dict, domain: Optional[str] = None) -> bool:
    """
    Upsert по inn: UPDATE … WHERE inn; если 0 строк — INSERT.
    Если нет соединения/таблицы — тихо выходим (best-effort).
    """
    if not await clients_requests_exists():
        return False

    row = _prepare_row_from_summary(summary, domain=domain)
    if not row.get("inn"):
        log.info("Не указан ИНН — запись в clients_requests пропущена.")
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

    eng = get_parsing_engine()
    if eng is None:
        return False

    try:
        async with eng.begin() as conn:
            res = await conn.execute(sql_update, row)
            if getattr(res, "rowcount", 0) == 0:
                await conn.execute(sql_insert, row)
        log.info("clients_requests upsert: inn=%s, domain_1=%s", row["inn"], row["domain_1"])
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("clients_requests upsert не удался (inn=%s): %s", row.get("inn"), e)
        return False


# ---------- INSERT into pars_site ----------

async def pars_site_insert_chunks(
    *,
    company_id: int,
    domain_1: str,
    url: str,
    chunks: Iterable[Mapping[str, Any] | tuple[int, int, str]],
    batch_size: int = 500,
) -> int:
    """
    Массовая вставка чанков в public.pars_site.

    Ожидается, что каждый чанк — либо dict с полями:
      - start: int
      - end: int
      - text: str
    либо tuple(start, end, text).

    Дубликаты по (company_id, domain_1, url, start, end) не вставляются
    (делаем WHERE NOT EXISTS для совместимости даже без уникального индекса).
    Возвращает количество реально вставленных записей.
    """
    eng = get_parsing_engine()
    if eng is None:
        return 0

    dom = _normalize_domain(domain_1) or domain_1
    inserted = 0

    def _coerce_chunk(ch: Mapping[str, Any] | tuple[int, int, str]) -> Optional[dict]:
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

    # Подготовка батча
    buf: list[dict] = []
    async with eng.begin() as conn:
        for ch in chunks:
            row = _coerce_chunk(ch)
            if not row:
                continue
            buf.append(row)
            if len(buf) >= batch_size:
                inserted += await _flush_pars_site_batch(conn, buf)
                buf.clear()
        if buf:
            inserted += await _flush_pars_site_batch(conn, buf)
            buf.clear()

    return inserted


async def _flush_pars_site_batch(conn, rows: list[dict]) -> int:
    """
    Вставка батча через INSERT ... SELECT WHERE NOT EXISTS,
    чтобы не зависеть от наличия UNIQUE индекса.
    """
    if not rows:
        return 0

    sql = text(
        """
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
        """
    )

    total = 0
    for row in rows:
        res = await conn.execute(sql, row)
        # rowcount в SA 2.x для INSERT обычно 1 или 0 (если NOT EXISTS сработал)
        try:
            total += int(getattr(res, "rowcount", 0) or 0)
        except Exception:
            pass
    return total
