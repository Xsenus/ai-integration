# app/services/dadata_client.py
from __future__ import annotations

import re
from typing import Any

import httpx
from app.config import settings

DADATA_SUGGEST_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"


def _ascii_only(value: str | None) -> str:
    """
    httpx требует ASCII в header values. Удаляем всё вне 0x20..0x7E.
    """
    if not value:
        return ""
    return re.sub(r"[^\x20-\x7E]", "", str(value))


async def find_party_by_inn(inn: str) -> dict[str, Any] | None:
    # Если ключи не заданы — бросаем RequestError, чтобы роуты поймали httpx.HTTPError
    if not settings.DADATA_API_KEY or not settings.DADATA_SECRET_KEY:
        req = httpx.Request("POST", DADATA_SUGGEST_URL)
        raise httpx.RequestError("DaData API keys are not configured", request=req)

    # Заголовки → ASCII
    raw_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {str(settings.DADATA_API_KEY).strip()}",
        "X-Secret": str(settings.DADATA_SECRET_KEY).strip(),
        "User-Agent": "ai-integration/1.0 (+https://example.com)",
    }
    headers = {k: _ascii_only(v) for k, v in raw_headers.items() if v is not None and _ascii_only(v) != ""}

    timeout = httpx.Timeout(10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(DADATA_SUGGEST_URL, headers=headers, json={"query": str(inn).strip()})
        resp.raise_for_status()
        data = resp.json() or {}
        sugs = data.get("suggestions") or []
        return sugs[0] if sugs else None
