from __future__ import annotations

import logging
import time
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, status

from app.api.analyze_json import _get_http_client
from app.config import settings

log = logging.getLogger("api.billing")
router = APIRouter(prefix="/v1/billing", tags=["billing"])

_CACHE_TTL_SECONDS = 60
_billing_remaining_cache: dict[str, Any] = {"timestamp": 0.0, "payload": None}


@router.get("/remaining")
async def billing_remaining() -> Any:
    base_url = (settings.ai_site_analyzer_url or "").strip()
    if not base_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ANALYZE_BASE is not configured",
        )

    now_ts = time.time()
    cached_at = float(_billing_remaining_cache.get("timestamp") or 0.0)
    cached_payload = _billing_remaining_cache.get("payload")
    if cached_payload is not None and (now_ts - cached_at) < _CACHE_TTL_SECONDS:
        return cached_payload

    target = f"{base_url.rstrip('/')}/v1/billing/remaining"
    client = await _get_http_client()

    try:
        response = await client.get(target)
    except httpx.RequestError as exc:
        log.warning("billing proxy request failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="ai-site-analyzer unavailable: failed to reach /v1/billing/remaining",
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="ai-site-analyzer returned non-JSON response for /v1/billing/remaining",
        ) from exc

    if response.status_code >= 400:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "ai-site-analyzer returned error for /v1/billing/remaining",
                "upstream_status": response.status_code,
                "upstream_payload": payload,
            },
        )

    _billing_remaining_cache["timestamp"] = now_ts
    _billing_remaining_cache["payload"] = payload

    return payload
