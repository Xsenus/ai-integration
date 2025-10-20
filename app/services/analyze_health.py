from __future__ import annotations

import asyncio
import logging
from typing import Mapping, Optional

import httpx

from app.config import settings

log = logging.getLogger("services.analyze_health")

_client_pool: dict[str, httpx.AsyncClient] = {}
_client_lock = asyncio.Lock()


class AnalyzeServiceUnavailable(RuntimeError):
    """Исключение при недоступности внешнего сервиса анализа."""


def normalize_analyze_base(url: Optional[str]) -> Optional[str]:
    """Нормализует базовый URL сервиса анализа (добавляет схему, обрезает слеши)."""

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


async def get_analyze_http_client(base_url: str) -> httpx.AsyncClient:
    normalized = normalize_analyze_base(base_url)
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


async def _ensure_health(client: httpx.AsyncClient, *, label: str) -> None:
    """Проверяет `/health` внешнего сервиса и бросает исключение при сбое."""

    try:
        response = await client.get("/health")
    except httpx.HTTPError as exc:  # noqa: BLE001
        log.warning("analyze-health: health check request failed (%s): %s", label, exc)
        raise AnalyzeServiceUnavailable("Нет доступа к сервису") from exc

    if response.status_code >= 500:
        log.warning(
            "analyze-health: health check returned %s (%s)",
            response.status_code,
            label,
        )
        raise AnalyzeServiceUnavailable("Нет доступа к сервису")

    if response.status_code >= 400:
        log.warning(
            "analyze-health: health check error %s (%s): %s",
            response.status_code,
            label,
            response.text,
        )
        raise AnalyzeServiceUnavailable("Нет доступа к сервису")

    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, Mapping) and payload.get("ok") is False:
        log.warning("analyze-health: health check reported not ok (%s)", label)
        raise AnalyzeServiceUnavailable("Нет доступа к сервису")

    log.info("analyze-health: health check ok (%s)", label)


async def ensure_service_available(base_url: str, *, label: str) -> None:
    """Проверяет доступность внешнего сервиса анализа."""

    try:
        client = await get_analyze_http_client(base_url)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "analyze-health: failed to prepare HTTP client for health check (%s): %s",
            label,
            exc,
        )
        raise AnalyzeServiceUnavailable("Нет доступа к сервису") from exc

    await _ensure_health(client, label=label)


async def probe_analyze_service(
    *, base_url: Optional[str] = None, label: str = "analyze-health:probe"
) -> dict[str, object]:
    """Пробует выполнить health-check внешнего сервиса и возвращает результат."""

    target = base_url or settings.analyze_base
    if not target:
        detail = "ANALYZE_BASE is not configured"
        log.warning("analyze-health: %s (%s)", detail, label)
        return {"ok": False, "detail": detail}

    try:
        client = await get_analyze_http_client(target)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "analyze-health: failed to prepare HTTP client (%s @ %s): %s",
            label,
            target,
            exc,
        )
        return {"ok": False, "detail": "Нет доступа к сервису"}

    try:
        await _ensure_health(client, label=label)
    except AnalyzeServiceUnavailable as exc:
        return {"ok": False, "detail": str(exc)}

    return {"ok": True, "detail": None}


__all__ = [
    "AnalyzeServiceUnavailable",
    "ensure_service_available",
    "get_analyze_http_client",
    "normalize_analyze_base",
    "probe_analyze_service",
]
