from __future__ import annotations
import re
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


async def find_party_by_inn(inn: str) -> dict | None:
    if not settings.DADATA_API_KEY or not settings.DADATA_SECRET_KEY:
        raise RuntimeError("DaData API keys are not configured")

    # Сначала формируем, затем прогоняем через ASCII-санитайзер.
    raw_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {str(settings.DADATA_API_KEY).strip()}",
        "X-Secret": str(settings.DADATA_SECRET_KEY).strip(),
        # Явно зададим ASCII-only User-Agent:
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
