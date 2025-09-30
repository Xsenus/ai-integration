from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
from app.config import settings

log = logging.getLogger(__name__)

B24_BASE_URL = (settings.B24_BASE_URL or "").rstrip("/") + "/"
PAGE_SIZE = int(settings.B24_PAGE_SIZE or 200)
BATCH_SIZE = int(getattr(settings, "B24_BATCH_SIZE", 25) or 25)   # ≤ 50 по правилам Bitrix
BATCH_ENABLED = bool(getattr(settings, "B24_BATCH_ENABLED", True))

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
    Преобразует dict в query-string, поддерживая вложенные словари в стиле Bitrix:
      {"order":{"ID":"ASC"},"select":["*","UF_*"],"start":0}
    -> order[ID]=ASC&select[]=*&select[]=UF_*, start=0
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

# -------------------- Обычный итератор (на всякий случай) --------------------

async def _call(method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return await _post(method, params or {})

async def iter_companies(all_props: bool = True) -> AsyncIterator[Dict[str, Any]]:
    """Одиночный постраничный перебор (без batch)."""
    start: int | str = 0
    select = ["*", "UF_*"] if all_props else ["ID", "TITLE", "DATE_MODIFY"]
    while True:
        payload = await _call("crm.company.list", {
            "order": {"ID": "ASC"},
            "filter": {},
            "select": select,
            "start": start,
        })
        result: List[Dict[str, Any]] = payload.get("result", []) or []
        if not result:
            break
        for item in result:
            yield item
        if "next" in payload:
            start = payload["next"]
            try:
                await asyncio.sleep(0.2)
            except Exception:
                pass
        else:
            break

# -------------------- Batch: множественные crm.company.list за раз --------------------

async def _batch(cmd: Dict[str, str], halt: int = 0) -> Dict[str, Any]:
    """Вызов метода batch. cmd — словарь: имя_команды -> 'метод?qs'."""
    payload = {"halt": halt, "cmd": cmd}
    return await _post("batch", payload)

async def iter_companies_batch(all_props: bool = True) -> AsyncIterator[Dict[str, Any]]:
    """
    Быстрый перебор всех компаний с помощью batch.
    Идея: за 1 запрос выполняем до BATCH_SIZE страниц crm.company.list.
    Затем двигаем «окно» start далее.
    """
    select = ["*", "UF_*"] if all_props else ["ID", "TITLE", "DATE_MODIFY"]
    start_values: List[int] = []  # стартовые смещения активного окна
    # Первое окно
    start0 = 0
    start_values = [start0 + i * PAGE_SIZE for i in range(BATCH_SIZE)]

    while True:
        # Сформировать команды на текущее окно
        cmd: Dict[str, str] = {}
        for idx, s in enumerate(start_values):
            params = {
                "order": {"ID": "ASC"},
                "filter": {},
                "select": select,
                "start": s,
            }
            qs = _qs(params)
            cmd[f"p{idx}"] = f"crm.company.list?{qs}"

        data = await _batch(cmd, halt=0)
        results: Dict[str, Any] = data.get("result", {}) or {}
        result_items: List[List[Dict[str, Any]]] = results.get("result", []) or []
        result_next: Dict[str, Any] = results.get("result_next", {}) or {}

        # Собираем данные и информацию о следующих страницах
        aggregated: List[Dict[str, Any]] = []
        next_values: List[int] = []
        # Важно: порядок p0..pN
        for idx in range(len(start_values)):
            key = f"p{idx}"
            page_items = []
            if isinstance(result_items, list) and idx < len(result_items):
                page_items = result_items[idx] or []
            elif isinstance(results.get("result", {}), dict):
                page_items = results["result"].get(key, []) or []

            for item in page_items:
                aggregated.append(item)

            # Если есть next для этой подкоманды — добавим его в следующее окно
            if key in result_next:
                try:
                    nv = int(result_next[key])
                    next_values.append(nv)
                except Exception:
                    pass

        # Отдаём собранные элементы
        if aggregated:
            for item in aggregated:
                yield item

        # Определяем следующее окно:
        # - если Bitrix вернул next хотя бы для одной подкоманды — продолжаем этим набором next’ов
        # - иначе — достигли конца
        if next_values:
            # Чтобы держать размер окна, добавим «ступеньки» дальше от максимального next
            # (обычно Bitrix возвращает “плотные” next, так что этого хватит)
            max_next = max(next_values)
            # Заполним окно до BATCH_SIZE
            while len(next_values) < BATCH_SIZE:
                max_next += PAGE_SIZE
                next_values.append(max_next)
            start_values = next_values[:BATCH_SIZE]
            # лёгкая пауза, чтобы не засыпать API
            try:
                await asyncio.sleep(0.2)
            except Exception:
                pass
        else:
            break

# Удобный селектор, чтобы не менять остальной код:
async def iter_companies_fast(all_props: bool = True) -> AsyncIterator[Dict[str, Any]]:
    if BATCH_ENABLED:
        async for x in iter_companies_batch(all_props=all_props):
            yield x
    else:
        async for x in iter_companies(all_props=all_props):
            yield x
