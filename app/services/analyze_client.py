from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

import httpx

from app.config import settings

log = logging.getLogger("services.analyze_client")

_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY = 0.5

_client_pool: dict[str, httpx.AsyncClient] = {}
_client_lock = asyncio.Lock()

def _normalize_base(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    base = url.strip()
    if not base:
        return None
    if not base.startswith(("http://", "https://")):
        base = "http://" + base
    return base.rstrip("/")


def _build_timeout() -> httpx.Timeout:
    timeout_s = max(1, int(settings.analyze_timeout or 30))
    connect = min(float(timeout_s), 10.0)
    write = min(float(timeout_s), 10.0)
    read = float(timeout_s)
    return httpx.Timeout(timeout_s, connect=connect, read=read, write=write)


async def _get_client(base_url: str) -> httpx.AsyncClient:
    normalized = _normalize_base(base_url)
    if not normalized:
        raise ValueError("Empty base URL")

    async with _client_lock:
        client = _client_pool.get(normalized)
        if client is not None:
            return client

        transport = httpx.AsyncHTTPTransport(retries=2)
        client = httpx.AsyncClient(
            base_url=normalized,
            timeout=_build_timeout(),
            transport=transport,
        )
        _client_pool[normalized] = client
        return client


def _coerce_vector(value: Any) -> Optional[list[float]]:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("data") or value.get("vector") or value.get("embedding")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:  # noqa: BLE001
            parsed = None
        if parsed is not None:
            value = parsed
    if not isinstance(value, (list, tuple)):
        return None
    result: list[float] = []
    for item in value:
        try:
            result.append(float(item))
        except (TypeError, ValueError):
            continue
    return result or None
async def _post_with_retries(
    base_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    label: str,
) -> httpx.Response | None:
    try:
        client = await _get_client(base_url)
    except Exception as exc:  # noqa: BLE001
        log.warning("analyze-client: не удалось создать HTTP-клиент (%s): %s", label, exc)
        return None

    last_error: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = await client.post(path, json=payload)
        except httpx.HTTPError as exc:  # noqa: BLE001
            last_error = exc
            log.warning(
                "analyze-client: попытка %s/%s не удалась (%s @ %s): %s",
                attempt,
                _MAX_ATTEMPTS,
                label,
                base_url,
                exc,
            )
        else:
            if response.status_code >= 500:
                log.warning(
                    "analyze-client: сервис вернул %s (%s @ %s)",
                    response.status_code,
                    label,
                    base_url,
                )
            else:
                return response
        await asyncio.sleep(min(2.0, _RETRY_BASE_DELAY * attempt))

    if last_error is not None:
        log.warning(
            "analyze-client: все попытки исчерпаны (%s @ %s): %s",
            label,
            base_url,
            last_error,
        )
    return None


async def fetch_site_description(
    text: str,
    *,
    embed_model: Optional[str],
    label: str,
) -> tuple[Optional[str], Optional[list[float]]]:
    payload: dict[str, Any] = {
        "source_text": text,
        "return_prompt": False,
    }
    chat_model = (settings.CHAT_MODEL or "").strip()
    if chat_model:
        payload["chat_model"] = chat_model

    base_url = _normalize_base(settings.analyze_base)
    if not base_url:
        log.warning("analyze-client: ANALYZE_BASE не настроен (%s)", label)
        return None, None

    response = await _post_with_retries(base_url, "/v1/site-profile", payload, label=label)
    if response is None:
        return None, None
    if response.status_code >= 400:
        log.warning(
            "analyze-client: внешний сервис вернул %s (%s @ %s)",
            response.status_code,
            label,
            base_url,
        )
        return None, None
    try:
        data = response.json()
    except ValueError:  # noqa: BLE001
        log.warning("analyze-client: не-JSON ответ (%s @ %s)", label, base_url)
        return None, None
    description_raw = data.get("description")
    description = (
        str(description_raw).strip()
        if isinstance(description_raw, str) and description_raw.strip()
        else None
    )
    vector = _coerce_vector(data.get("description_vector"))

    if vector is None:
        literal = data.get("description_vector_literal") or data.get("vector_literal")
        if isinstance(literal, str):
            vector = _coerce_vector(literal)
    if description:
        log.info(
            "analyze-client: описание получено (%s, base=%s, vector=%s)",
            label,
            base_url,
            bool(vector),
        )
    else:
        log.info("analyze-client: описание отсутствует (%s, base=%s)", label, base_url)
    return description, vector


async def fetch_embedding(text: str, *, label: str) -> Optional[list[float]]:
    payload = {"q": text}
    base_url = _normalize_base(settings.analyze_base)
    if not base_url:
        log.warning("analyze-client: ANALYZE_BASE не настроен (%s)", label)
        return None

    response = await _post_with_retries(base_url, "/ai-search", payload, label=label)
    if response is None:
        return None
    if response.status_code >= 400:
        log.warning(
            "analyze-client: embedding сервис вернул %s (%s @ %s)",
            response.status_code,
            label,
            base_url,
        )
        return None
    try:
        data = response.json()
    except ValueError:  # noqa: BLE001
        log.warning("analyze-client: embedding ответ не-JSON (%s @ %s)", label, base_url)
        return None
    vector = _coerce_vector(
        data.get("embedding")
        or data.get("vector")
        or data.get("description_vector")
    )
    if vector:
        log.info(
            "analyze-client: embedding получен (%s, base=%s, size=%s)",
            label,
            base_url,
            len(vector),
        )
        return vector
    log.info("analyze-client: embedding пустой (%s, base=%s)", label, base_url)
    return None


__all__ = [
    "fetch_site_description",
    "fetch_embedding",
]
