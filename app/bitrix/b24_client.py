from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
from app.config import settings

log = logging.getLogger(__name__)
http_log = logging.getLogger("bitrix.http")

# Конфиг
B24_BASE_URL = (settings.B24_BASE_URL or "").rstrip("/") + "/"
# У crm.company.list дефолтный лимит 50; оставим настраиваемым.
PAGE_LIMIT = int(getattr(settings, "B24_PAGE_LIMIT", 50) or 50)

# Batch-параметры
BATCH_HARD_MAX = 25  # Жёсткий лимит Bitrix: не более 25 команд в одном batch-запросе
BATCH_SIZE = min(int(getattr(settings, "B24_BATCH_SIZE", 25) or 25), BATCH_HARD_MAX)
BATCH_ENABLED = bool(getattr(settings, "B24_BATCH_ENABLED", True))
BATCH_PAUSE_MS = int(getattr(settings, "B24_BATCH_PAUSE_MS", 500) or 500)  # пауза между batch-запросами

# ---------- Вспомогательные ----------

def _ensure_url() -> None:
    if not settings.B24_BASE_URL:
        raise RuntimeError("B24_BASE_URL is not configured")


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0)


def _redact_url(u: str) -> str:
    # маскируем секрет вебхука: оставим первые 4 символа токена
    try:
        parts = u.split("/rest/")
        if len(parts) == 2:
            left, right = parts
            segs = right.strip("/").split("/")
            if len(segs) >= 2:
                user_id, token = segs[0], segs[1]
                safe = (token[:4] + "…") if token else ""
                segs[1] = safe
                return f"{left}/rest/" + "/".join(segs) + "/"
    except Exception:
        pass
    return u


async def _post(method: str, json: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_url()
    url = f"{B24_BASE_URL}{method}"
    red = _redact_url(url)
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=_timeout()) as client:
        if settings.B24_LOG_VERBOSE or settings.B24_LOG_BODIES:
            body_preview = str(json)
            if not settings.B24_LOG_BODIES:
                body_preview = "{...}"  # не пишем тело, если флаг выключен
            else:
                body_preview = body_preview[: settings.B24_LOG_BODY_CHARS]
            http_log.info("POST %s json=%s", red, body_preview)

        resp = await client.post(url, json=json)
        resp.raise_for_status()

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        data = resp.json()

        # Короткий сводный лог по ответу
        if isinstance(data, dict):
            if method == "batch":
                r = data.get("result", {}) or {}
                r_res = r.get("result", {})
                r_next = r.get("result_next", {})
                res_len = len(r_res) if isinstance(r_res, dict) else (
                    len(r_res) if isinstance(r_res, list) else 0
                )
                next_len = len(r_next) if isinstance(r_next, dict) else 0
                http_log.info(
                    "RESP %s %s in %dms (result=%s keys, result_next=%s keys)",
                    red,
                    resp.status_code,
                    elapsed_ms,
                    res_len,
                    next_len,
                )
            else:
                res = data.get("result")
                res_len = len(res) if isinstance(res, list) else (1 if res is not None else 0)
                nxt = data.get("next", None)
                http_log.info(
                    "RESP %s %s in %dms (items=%s, next=%s)",
                    red,
                    resp.status_code,
                    elapsed_ms,
                    res_len,
                    nxt,
                )
        else:
            http_log.info("RESP %s %s in %dms", red, resp.status_code, elapsed_ms)

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


async def iter_companies(all_props: bool = True, max_items: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
    """
    Последовательный перебор crm.company.list без batch.
    """
    start: int | str | None = 0
    select = ["*", "UF_*"] if all_props else ["ID", "TITLE", "DATE_MODIFY"]

    seen_starts: set[int] = set()
    yielded = 0

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

        payload = await _call(
            "crm.company.list",
            {
                "order": {"ID": "ASC"},
                "filter": {},
                "select": select,
                "start": start,
            },
        )
        items: List[Dict[str, Any]] = payload.get("result", []) or []
        for item in items:
            yield item
            yielded += 1
            if max_items and yielded >= max_items:
                log.info("B24(non-batch): reached max_items=%s — stopping.", max_items)
                return

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


async def iter_companies_batch(all_props: bool = True, max_items: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
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
    base_qs = _qs(
        {
            "order": {"ID": "ASC"},
            "filter": {},
            "select": select,
        }
    )

    current_start: int | None = 0
    yielded = 0

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
                yielded += 1
                if max_items and yielded >= max_items:
                    log.info("B24(batch): reached max_items=%s — stopping.", max_items)
                    return

            pages_with_items += 1
            last_page_count = len(page_items)

        # стоп №1: хвост
        if pages_with_items == 0 or last_page_count < PAGE_LIMIT:
            log.info(
                "B24(batch): tail reached (pages_with_items=%d, last_count=%d < PAGE_LIMIT=%d).",
                pages_with_items,
                last_page_count,
                PAGE_LIMIT,
            )
            break

        # вычисляем следующий оффсет монотонно
        next_start = current_start + pages_with_items * PAGE_LIMIT
        if settings.B24_LOG_VERBOSE:
            log.info(
                "B24(batch): current_start=%d, pages_with_items=%d, last_count=%d, next_start=%d",
                current_start,
                pages_with_items,
                last_page_count,
                next_start,
            )

        # стоп №2: защита от зацикливания/регресса
        if next_start <= current_start:
            log.warning(
                "B24(batch): computed next_start=%d <= current_start=%d — stopping.",
                next_start,
                current_start,
            )
            break

        current_start = next_start

        # Пауза между batch-запросами (настраиваемая)
        if BATCH_PAUSE_MS > 0:
            try:
                await asyncio.sleep(BATCH_PAUSE_MS / 1000.0)
            except Exception:
                pass


# ---------- Унифицированный селектор ----------

async def iter_companies_fast(all_props: bool = True, max_items: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
    """
    Возвращает итератор компаний: batch или обычный — в зависимости от конфигурации.
    """
    if BATCH_ENABLED:
        async for x in iter_companies_batch(all_props=all_props, max_items=max_items):
            yield x
    else:
        async for x in iter_companies(all_props=all_props, max_items=max_items):
            yield x
