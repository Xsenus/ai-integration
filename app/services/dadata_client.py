from __future__ import annotations
import httpx
from app.config import settings

DADATA_SUGGEST_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"

async def find_party_by_inn(inn: str) -> dict | None:
    if not settings.DADATA_API_KEY or not settings.DADATA_SECRET_KEY:
        raise RuntimeError("DaData API keys are not configured")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {settings.DADATA_API_KEY}",
        "X-Secret": settings.DADATA_SECRET_KEY,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(DADATA_SUGGEST_URL, headers=headers, json={"query": inn})
        resp.raise_for_status()
        data = resp.json() or {}
        sugs = data.get("suggestions") or []
        return sugs[0] if sugs else None
