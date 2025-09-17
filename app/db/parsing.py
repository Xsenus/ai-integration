from __future__ import annotations

import logging
from typing import Any, Optional, Sequence
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from ..config import settings

log = logging.getLogger("dadata-bitrix")

engine_parsing: AsyncEngine | None = None


def get_parsing_engine() -> AsyncEngine:
    """Lazy-инициализация engine для parsing_data."""
    global engine_parsing
    if engine_parsing is None:
        engine_parsing = create_async_engine(settings.parsing_url, pool_pre_ping=True)
    return engine_parsing


async def table_exists(table_qualified: str) -> bool:
    """
    Проверяет наличие таблицы в parsing_data.
    Принимает 'public.clients_requests' или 'public.pars_site'.
    """
    try:
        engine = get_parsing_engine()
        async with engine.connect() as conn:
            res = await conn.execute(text("SELECT to_regclass(:tname)"), {"tname": table_qualified})
            return res.scalar_one_or_none() is not None
    except Exception as e:
        log.info("Нет соединения с parsing_data (%s) — пропускаю операции для %s.", e, table_qualified)
        return False

async def pars_site_exists() -> bool:
    return await table_exists("public.pars_site")

async def get_clients_request_id(inn: str, domain_1: Optional[str] = None) -> Optional[int]:
    """
    Ищет id в public.clients_requests по ИНН и, опционально, domain_1.
    Если domain_1 не задан, берём последний по времени id.
    """
    engine = get_parsing_engine()
    if domain_1:
        sql = text("""
            SELECT id FROM public.clients_requests
            WHERE inn = :inn AND domain_1 = :domain_1
            ORDER BY id DESC LIMIT 1
        """)
        params = {"inn": inn, "domain_1": _normalize_domain(domain_1)}
    else:
        sql = text("""
            SELECT id FROM public.clients_requests
            WHERE inn = :inn
            ORDER BY id DESC LIMIT 1
        """)
        params = {"inn": inn}

    async with engine.begin() as conn:
        res = await conn.execute(sql, params)
        row = res.first()
        return int(row[0]) if row else None

async def clients_requests_exists() -> bool:
    """
    Проверяем, что есть соединение и существует таблица public.clients_requests.
    НИЧЕГО не создаём, только проверяем.
    """
    try:
        engine = get_parsing_engine()
        async with engine.connect() as conn:
            res = await conn.execute(text("SELECT to_regclass('public.clients_requests')"))
            reg = res.scalar_one_or_none()
            if reg is None:
                log.info("В parsing_data таблица public.clients_requests не найдена — пропускаю запись.")
                return False
            return True
    except Exception as e:
        log.info("Нет соединения с parsing_data (%s) — пропускаю запись в clients_requests.", e)
        return False


def _okved_text(item: Any) -> str | None:
    """Превращаем элемент okved в читаемый текст 'код — наименование'."""
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
    """Нормализуем домен: вырезаем протокол/путь, берём host, в нижний регистр, без www."""
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
    """Готовим словарь параметров под INSERT в clients_requests."""
    emails = summary.get("emails") or []
    if isinstance(emails, list):
        emails_str = ", ".join(str(e) for e in emails if e)
    else:
        emails_str = str(emails) if emails else None

    main_okved = summary.get("main_okved")
    okveds = summary.get("okveds") or []

    # убрать дубликат главного ОКВЭД из вторичных (если коды совпадают)
    def _get_code(x):
        return x.get("code") or x.get("value") or x.get("okved") if isinstance(x, dict) else str(x)

    secondaries = []
    for it in okveds:
        code = _get_code(it)
        if main_okved and code and str(code) == str(main_okved):
            continue
        secondaries.append(_okved_text(it))
    secondaries = [x for x in secondaries if x]          # убрать None/пустые
    secondaries = (secondaries + [None] * 7)[:7]         # до 7 шт

    row = {
        "company_name": summary.get("short_name"),
        "inn": summary.get("inn"),
        "domain_1": _normalize_domain(domain),  # <-- здесь ставим переданный домен (или None)
        "domain_2": emails_str,                 # emails списком → строка
        "okved_main": main_okved,
        "okved_vtor_1": secondaries[0],
        "okved_vtor_2": secondaries[1],
        "okved_vtor_3": secondaries[2],
        "okved_vtor_4": secondaries[3],
        "okved_vtor_5": secondaries[4],
        "okved_vtor_6": secondaries[5],
        "okved_vtor_7": secondaries[6],
    }
    return row


async def push_clients_request(summary: dict, domain: Optional[str] = None) -> bool:
    """
    Upsert по inn: сначала UPDATE, если затронуто 0 строк — INSERT.
    Если нет соединения/таблицы — тихо выходим (best-effort).
    """
    available = await clients_requests_exists()
    if not available:
        return False

    row = _prepare_row_from_summary(summary, domain=domain)
    if not row.get("inn"):
        log.info("Не указан ИНН — запись в clients_requests пропущена.")
        return False

    update_sql = text("""
        UPDATE public.clients_requests
        SET company_name  = :company_name,
            domain_1      = COALESCE(:domain_1, domain_1),
            domain_2      = :domain_2,
            okved_main    = :okved_main,
            okved_vtor_1  = :okved_vtor_1,
            okved_vtor_2  = :okved_vtor_2,
            okved_vtor_3  = :okved_vtor_3,
            okved_vtor_4  = :okved_vtor_4,
            okved_vtor_5  = :okved_vtor_5,
            okved_vtor_6  = :okved_vtor_6,
            okved_vtor_7  = :okved_vtor_7
        WHERE inn = :inn
    """)

    insert_sql = text("""
        INSERT INTO public.clients_requests
        (company_name, inn, domain_1, domain_2, okved_main,
         okved_vtor_1, okved_vtor_2, okved_vtor_3, okved_vtor_4, okved_vtor_5, okved_vtor_6, okved_vtor_7)
        VALUES
        (:company_name, :inn, :domain_1, :domain_2, :okved_main,
         :okved_vtor_1, :okved_vtor_2, :okved_vtor_3, :okved_vtor_4, :okved_vtor_5, :okved_vtor_6, :okved_vtor_7)
    """)

    try:
        engine = get_parsing_engine()
        async with engine.begin() as conn:
            res = await conn.execute(update_sql, row)
            if getattr(res, "rowcount", 0) == 0:
                await conn.execute(insert_sql, row)
        log.info("clients_requests upsert для ИНН %s выполнен (domain_1=%s)", row["inn"], row["domain_1"])
        return True
    except Exception as e:
        log.warning("clients_requests upsert не удался (ИНН %s): %s", row.get("inn"), e)
        return False


# =========================
# Новое: явное создание clients_requests с возвратом id (+fallback)
# =========================

async def create_clients_request(company_name: str, inn: str, domain_1: Optional[str]) -> int:
    """
    Явно создаём запись в clients_requests и возвращаем её id.
    Если запись уже существует (конфликт уникальности на твоей стороне),
    делаем fallback: ищем id по (inn, domain_1).
    """
    engine = get_parsing_engine()
    insert_sql = text("""
        INSERT INTO public.clients_requests (company_name, inn, domain_1)
        VALUES (:company_name, :inn, :domain_1)
        RETURNING id
    """)
    select_sql = text("""
        SELECT id
        FROM public.clients_requests
        WHERE inn = :inn
          AND (:domain_1 IS NULL OR domain_1 = :domain_1)
        ORDER BY id DESC
        LIMIT 1
    """)

    domain_norm = _normalize_domain(domain_1)

    try:
        async with engine.begin() as conn:
            res = await conn.execute(insert_sql, {
                "company_name": company_name,
                "inn": inn,
                "domain_1": domain_norm,
            })
            new_id = res.scalar_one()
            log.info("clients_requests: создан id=%s (inn=%s, domain_1=%s)", new_id, inn, domain_norm)
            return int(new_id)
    except SQLAlchemyError as e:
        log.info("create_clients_request: insert conflict (%s), пробуем SELECT id", e)

    async with engine.begin() as conn:
        res = await conn.execute(select_sql, {"inn": inn, "domain_1": domain_norm})
        row = res.first()
        if not row:
            raise RuntimeError("Не удалось создать/найти clients_requests.id")
        found_id = int(row[0])
        log.info("clients_requests: найден id=%s для (inn=%s, domain_1=%s)", found_id, inn, domain_norm)
        return found_id


# =========================
# Новое: массовая вставка чанков в pars_site
# =========================

async def insert_pars_site_chunks(
    company_id: int,
    domain_1: str,
    url: str,
    chunks: list[str],
    batch_size: int = 500,
) -> int:
    """
    Вставляет чанки в pars_site:
      (company_id, domain_1, text_par, url, created_at DEFAULT now(), text_vector NULL)
    Возвращает количество вставленных строк.

    Для надёжности делаем вставку батчами (во избежание ограничений executemany).
    """
    if not chunks:
        log.info("pars_site: список чанков пуст — вставка пропущена.")
        return 0

    engine = get_parsing_engine()
    sql = text("""
        INSERT INTO public.pars_site (company_id, domain_1, text_par, url, created_at, text_vector)
        VALUES (:company_id, :domain_1, :text_par, :url, NOW(), NULL)
    """)

    total = 0
    domain_norm = _normalize_domain(domain_1)
    url_val = url  # ожидаем полный URL главной страницы

    async with engine.begin() as conn:
        # Вставляем кусками, чтобы не уткнуться в лимиты драйвера
        for i in range(0, len(chunks), batch_size):
            part = chunks[i : i + batch_size]
            rows = [
                {
                    "company_id": company_id,
                    "domain_1": domain_norm,
                    "text_par": ch,
                    "url": url_val,
                }
                for ch in part
            ]
            # В SQLAlchemy 2.0 executemany достигается передачей списка параметров:
            await conn.execute(sql, rows)
            total += len(rows)

    log.info("pars_site: вставлено %s строк (company_id=%s, domain_1=%s, url=%s)", total, company_id, domain_norm, url_val)
    return total
