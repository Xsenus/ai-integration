from __future__ import annotations

import logging
from typing import Awaitable, Callable, Mapping, Optional

import httpx

log = logging.getLogger("services.analyze_health")

_HEALTH_PATH = "/health"
_HEALTH_ERROR = "Нет доступа к сервису"


class AnalyzeServiceUnavailable(RuntimeError):
    """Исключение при недоступности внешнего сервиса анализа."""


async def _request_health(client: httpx.AsyncClient, *, label: str, target: str | None) -> httpx.Response:
    try:
        response = await client.get(_HEALTH_PATH)
    except httpx.HTTPError as exc:  # noqa: BLE001
        log.warning(
            "analyze-health: health check request failed (%s, target=%s): %s",
            label,
            target,
            exc,
        )
        raise AnalyzeServiceUnavailable(_HEALTH_ERROR) from exc
    return response


def _validate_health_response(response: httpx.Response, *, label: str, target: str | None) -> None:
    status = response.status_code
    if status >= 500:
        log.warning(
            "analyze-health: health check returned %s (%s, target=%s)",
            status,
            label,
            target,
        )
        raise AnalyzeServiceUnavailable(_HEALTH_ERROR)
    if status >= 400:
        log.warning(
            "analyze-health: health check error %s (%s, target=%s): %s",
            status,
            label,
            target,
            response.text,
        )
        raise AnalyzeServiceUnavailable(_HEALTH_ERROR)

    payload: Optional[Mapping[str, object]]
    try:
        parsed = response.json()
    except ValueError:
        parsed = None

    if isinstance(parsed, Mapping):
        payload = parsed
    else:
        payload = None

    if payload is not None and payload.get("ok") is False:
        log.warning(
            "analyze-health: health check reported not ok (%s, target=%s)",
            label,
            target,
        )
        raise AnalyzeServiceUnavailable(_HEALTH_ERROR)

    log.info(
        "analyze-health: health check ok (%s, target=%s, status=%s)",
        label,
        target,
        status,
    )


async def ensure_analyze_client_health(
    client: httpx.AsyncClient,
    *,
    label: str,
    target: str | None = None,
) -> None:
    """Выполняет health-check через переданный HTTP-клиент."""

    response = await _request_health(client, label=label, target=target)
    _validate_health_response(response, label=label, target=target)


ClientFactory = Callable[[str], Awaitable[httpx.AsyncClient]]


async def ensure_analyze_service_available(
    base_url: str,
    *,
    label: str,
    client_factory: ClientFactory,
) -> None:
    """Создаёт HTTP-клиент через factory и проверяет /health."""

    try:
        client = await client_factory(base_url)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "analyze-health: failed to prepare HTTP client (%s, target=%s): %s",
            label,
            base_url,
            exc,
        )
        raise AnalyzeServiceUnavailable(_HEALTH_ERROR) from exc

    await ensure_analyze_client_health(client, label=label, target=base_url)


__all__ = [
    "AnalyzeServiceUnavailable",
    "ensure_analyze_client_health",
    "ensure_analyze_service_available",
]
