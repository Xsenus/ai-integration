from __future__ import annotations

import asyncio
import json
import re
import logging
from typing import Any, Optional

import httpx

from app.config import settings

log = logging.getLogger("services.analyze_client")

_DEFAULT_ANALYZE_BASE = "http://37.221.125.221:8123"
_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY = 0.5

_client_pool: dict[str, httpx.AsyncClient] = {}
_client_lock = asyncio.Lock()

_SECTION_RE = re.compile(r"\[(?P<name>[A-Z_]+)\]\s*=\s*\[(?P<value>.*?)\]", re.IGNORECASE | re.DOTALL)


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


def _extract_sections(answer: str) -> dict[str, str]:
    result: dict[str, str] = {}
    if not answer:
        return result

    for match in _SECTION_RE.finditer(answer):
        name = match.group("name").upper()
        value = match.group("value").strip()
        if value:
            result[name] = value
    return result


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
        "text_par": text,
        "return_prompt": False,
        "return_answer_raw": False,
    }
    if embed_model:
        payload["embed_model"] = embed_model

    bases: list[str] = []
    primary = _normalize_base(settings.analyze_base)
    fallback = _normalize_base(_DEFAULT_ANALYZE_BASE)
    if primary:
        bases.append(primary)
    if fallback and fallback not in bases:
        bases.append(fallback)

    for base in bases:
        response = await _post_with_retries(base, "/v1/analyze/json", payload, label=label)
        if response is None:
            continue
        if response.status_code >= 400:
            log.warning(
                "analyze-client: внешний сервис вернул %s (%s @ %s)",
                response.status_code,
                label,
                base,
            )
            continue
        try:
            data = response.json()
        except ValueError:  # noqa: BLE001
            log.warning("analyze-client: не-JSON ответ (%s @ %s)", label, base)
            continue
        description_raw = data.get("description")
        description = (
            str(description_raw).strip()
            if isinstance(description_raw, str) and description_raw.strip()
            else None
        )
        vector = _coerce_vector(
            data.get("description_vector")
            or data.get("vector")
            or data.get("description_embedding")
        )

        if not description:
            answer_raw = data.get("answer") or data.get("raw_answer") or data.get("result")
            if isinstance(answer_raw, str):
                sections = _extract_sections(answer_raw)
                maybe_description = sections.get("DESCRIPTION")
                if maybe_description:
                    description = maybe_description

        if vector is None:
            literal = data.get("description_vector_literal") or data.get("vector_literal")
            if isinstance(literal, str):
                vector = _coerce_vector(literal)

        if vector is None:
            sections = None
            answer_raw = data.get("answer") or data.get("raw_answer") or data.get("result")
            if isinstance(answer_raw, str):
                sections = sections or _extract_sections(answer_raw)
            if sections:
                literal = sections.get("DESCRIPTION_VECTOR") or sections.get("VECTOR")
                if literal:
                    vector = _coerce_vector(literal)
        if description:
            log.info(
                "analyze-client: описание получено (%s, base=%s, vector=%s)",
                label,
                base,
                bool(vector),
            )
        else:
            log.info("analyze-client: описание отсутствует (%s, base=%s)", label, base)
        return description, vector

    return None, None


async def fetch_embedding(text: str, *, label: str) -> Optional[list[float]]:
    payload = {"q": text}
    bases: list[str] = []
    primary = _normalize_base(settings.analyze_base)
    fallback = _normalize_base(_DEFAULT_ANALYZE_BASE)
    if primary:
        bases.append(primary)
    if fallback and fallback not in bases:
        bases.append(fallback)

    for base in bases:
        response = await _post_with_retries(base, "/ai-search", payload, label=label)
        if response is None:
            continue
        if response.status_code >= 400:
            log.warning(
                "analyze-client: embedding сервис вернул %s (%s @ %s)",
                response.status_code,
                label,
                base,
            )
            continue
        try:
            data = response.json()
        except ValueError:  # noqa: BLE001
            log.warning("analyze-client: embedding ответ не-JSON (%s @ %s)", label, base)
            continue
        vector = _coerce_vector(
            data.get("embedding")
            or data.get("vector")
            or data.get("description_vector")
        )
        if vector:
            log.info(
                "analyze-client: embedding получен (%s, base=%s, size=%s)",
                label,
                base,
                len(vector),
            )
            return vector
        log.info("analyze-client: embedding пустой (%s, base=%s)", label, base)

    return None


__all__ = [
    "fetch_site_description",
    "fetch_embedding",
]
