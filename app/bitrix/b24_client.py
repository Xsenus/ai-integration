from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
from app.config import settings

log = logging.getLogger(__name__)

# Конфиг
B24_BASE_URL = (settings.B24_BASE_URL or "").rstrip("/") + "/"
# У crm.company.list дефолтный лимит 50; оставим настраиваемым.
PAGE_LIMIT = int(getattr(settings, "B24_PAGE_LIMIT", 50) or 50)
BATCH_SIZE = int(getattr(settings, "B24_BATCH_SIZE", 25) or 25)     # ≤ 50
BATCH_ENABLED = bool(getattr(settings, "B24_BATCH_ENABLED", True))


# ---------- Вспомогательные ----------

def _ensure_url() -> None:
    if not settings.B24_BASE_URL:
        raise RuntimeError("B24_BASE_URL is not configured")


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0)


async def _post(method: str, json: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_url()
    url = f"{B24_BASE_URL}{method}"
    async with httpx.AsyncClient(timeout=_timeout()) as client:
        resp = await client.post(url, json=json)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Bitrix24 error: {data}")
        return data


def _qs(params: Dict[str, Any]) -> str:
    """
    Преобразует dict в query-string, как ожидает Bitrix:
      {"order":{"ID":"ASC"},"select":["*","UF_*"],"start":0}
      -> order[ID]=ASC&select[]=*&select[]=UF_*&start=0
    """
    flat: List[Tuple[str, Any]] = []

    def walk(prefix: str, val: Any):
        if isinstance(val, dict):
            for k, v in val.items():
                walk(f"{prefix}[{k}]", v)
        elif isinstance(val, (list, tuple)):
            for v in val:
                flat.append((f"{prefix}[]", v))
        else:
            flat.append((prefix, val))

    for k, v in params.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                walk(f"{k}[{kk}]", vv)
        elif isinstance(v, (list, tuple)):
            for vv in v:
                flat.append((f"{k}[]", vv))
        else:
            flat.append((k, v))
    return urlencode(flat, doseq=True)


# ---------- Обычный (последовательный) перебор без batch ----------

async def _call(method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return await _post(method, params or {})


async def iter_companies(all_props: bool = True) -> AsyncIterator[Dict[str, Any]]:
    """
    Последовательный перебор crm.company.list без batch.
    """
    start: int | str | None = 0
    select = ["*", "UF_*"] if all_props else ["ID", "TITLE", "DATE_MODIFY"]

    seen_starts: set[int] = set()

    while start is not None:
        # предохранитель от циклов
        try:
            s_int = int(start)
            if s_int in seen_starts:
                log.warning("B24: detected repeated start=%s in non-batch iterator — stopping.", s_int)
                break
            seen_starts.add(s_int)
        except Exception:
            pass

        payload = await _call("crm.company.list", {
            "order": {"ID": "ASC"},
            "filter": {},
            "select": select,
            "start": start,
        })
        items: List[Dict[str, Any]] = payload.get("result", []) or []
        for item in items:
            yield item

        # стоп: если хвост
        if len(items) < PAGE_LIMIT:
            log.info("B24(non-batch): tail reached (count=%d < PAGE_LIMIT=%d).", len(items), PAGE_LIMIT)
            break

        start = payload.get("next", None)
        try:
            await asyncio.sleep(0.2)
        except Exception:
            pass


# ---------- Batch: последовательная ЦЕПОЧКА страниц за 1 запрос ----------

async def _batch(cmd: Dict[str, str], halt: int = 0) -> Dict[str, Any]:
    """
    Вызов метода batch. cmd — словарь: имя_команды -> 'метод?qs'.
    """
    payload = {"halt": halt, "cmd": cmd}
    return await _post("batch", payload)


async def iter_companies_batch(all_props: bool = True) -> AsyncIterator[Dict[str, Any]]:
    """
    Надёжный перебор: последовательная цепочка в batch.
    За 1 HTTP-запрос вытягиваем до BATCH_SIZE подряд идущих страниц:
      p0: start=S
      p1: start=$result[p0][next]
      p2: start=$result[p1][next]
      ...
    Следующий оффсет вычисляем монотонно: current_start += (число непустых страниц) * PAGE_LIMIT.
    Это защищает от кейсов, когда Bitrix отдаёт неподвижный или «ломаный» next.
    Доп. предохранители:
      - если последняя непустая страница вернула < PAGE_LIMIT — завершаем (хвост);
      - если вычисленный next_start <= current_start — завершаем (защита от зацикливания).
    """
    select = ["*", "UF_*"] if all_props else ["ID", "TITLE", "DATE_MODIFY"]
    base_qs = _qs({
        "order": {"ID": "ASC"},
        "filter": {},
        "select": select,
    })

    current_start: int | None = 0

    while current_start is not None:
        # Собираем цепочку p0..pN
        cmd: Dict[str, str] = {}
        for idx in range(BATCH_SIZE):
            if idx == 0:
                start_part = f"start={current_start}"
            else:
                prev = idx - 1
                start_part = f"start=$result[p{prev}][next]"
            cmd[f"p{idx}"] = f"crm.company.list?{base_qs}&{start_part}"

        data = await _batch(cmd, halt=0)
        results: Dict[str, Any] = data.get("result", {}) or {}
        result_map: Dict[str, Any] = results.get("result", {}) or {}

        pages_with_items = 0
        last_page_count = 0

        for idx in range(BATCH_SIZE):
            key = f"p{idx}"
            page_items: List[Dict[str, Any]] = []
            if isinstance(result_map, dict):
                page_items = result_map.get(key, []) or []
            else:
                if isinstance(result_map, list) and idx < len(result_map):
                    page_items = result_map[idx] or []

            if not page_items:
                break

            for item in page_items:
                yield item

            pages_with_items += 1
            last_page_count = len(page_items)

        # стоп №1: хвост
        if pages_with_items == 0 or last_page_count < PAGE_LIMIT:
            log.info("B24(batch): tail reached (pages_with_items=%d, last_count=%d < PAGE_LIMIT=%d).",
                     pages_with_items, last_page_count, PAGE_LIMIT)
            break

        # вычисляем следующий оффсет монотонно
        next_start = current_start + pages_with_items * PAGE_LIMIT

        # стоп №2: защита от зацикливания/регресса
        if next_start <= current_start:
            log.warning("B24(batch): computed next_start=%d <= current_start=%d — stopping.", next_start, current_start)
            break

        current_start = next_start

        try:
            await asyncio.sleep(0.2)
        except Exception:
            pass


# ---------- Унифицированный селектор ----------

async def iter_companies_fast(all_props: bool = True) -> AsyncIterator[Dict[str, Any]]:
    """
    Возвращает итератор компаний: batch или обычный — в зависимости от конфигурации.
    """
    if BATCH_ENABLED:
        async for x in iter_companies_batch(all_props=all_props):
            yield x
    else:
        async for x in iter_companies(all_props=all_props):
            yield x
