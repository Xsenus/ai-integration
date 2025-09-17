from __future__ import annotations

import logging
from typing import Any, Optional
from urllib.parse import urlparse

from sqlalchemy import text
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
    """Нормализуем домен: вырезаем протокол/путь, берём host, в нижний регистр."""
    if not domain:
        return None
    s = domain.strip()
    if not s:
        return None
    parsed = urlparse(s if "://" in s else "http://" + s)
    host = (parsed.netloc or parsed.path).strip().lower().rstrip("/")
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
    Если есть соединение и таблица clients_requests — вставляем строку.
    Возвращаем True при успешной вставке, иначе False. Ошибки не пробрасываем наверх.
    """
    available = await clients_requests_exists()
    if not available:
        return False

    row = _prepare_row_from_summary(summary, domain=domain)
    if not row.get("inn"):
        log.info("Не указан ИНН — запись в clients_requests пропущена.")
        return False

    sql = text("""
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
            await conn.execute(sql, row)
        log.info("Добавлена запись в parsing_data.public.clients_requests для ИНН %s (domain_1=%s)",
                 row["inn"], row["domain_1"])
        return True
    except Exception as e:
        log.warning("Не удалось записать в clients_requests (ИНН %s): %s", row.get("inn"), e)
        return False
