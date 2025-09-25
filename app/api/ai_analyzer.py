from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import quote, urlparse

import httpx
from fastapi import APIRouter, HTTPException, status

from app.config import settings
from app.schemas.ai_analyzer import (
    AiAnalyzerRequest,
    AiAnalyzerResponse,
    CompanyBlock,
    AiBlock,
    AiProduct,
    AiEquipment,
)
from app.services.ai_analyzer import analyze_company_by_inn

log = logging.getLogger("api.ai_analyzer")
router = APIRouter(prefix="/v1/lookup", tags=["ai-analyzer"])


def _canonical_response(
    inn: str,
    *,
    domain1: Optional[str] = None,
    domain2: Optional[str] = None,
    industry: Optional[str] = None,
    sites: Optional[list[str]] = None,
    products: Optional[list[AiProduct]] = None,
    equipment: Optional[list[AiEquipment]] = None,
    utp: Optional[str] = None,
    letter: Optional[str] = None,
    note: Optional[str] = None,
) -> AiAnalyzerResponse:
    """Собирает ответ строго в каноническом формате."""
    sites = sites or [d for d in (domain1, domain2) if d]
    company = CompanyBlock(domain1=domain1, domain2=domain2)
    ai = AiBlock(
        industry=industry,
        sites=sites,
        products=products or [],
        equipment=equipment or [],
        utp=utp,
        letter=letter,
    )
    return AiAnalyzerResponse(ok=True, inn=inn, company=company, ai=ai, note=note)


def _build_site_path_param(site: str) -> str:
    """
    Для внешнего сервиса передаём ТОЛЬКО хост (netloc), без схемы и пути.
    Примеры:
      https://www.elteza.ru         -> www.elteza.ru
      https://elteza.ru/contacts    -> elteza.ru
      elteza.ru                     -> elteza.ru
      www.elteza.ru/                -> www.elteza.ru
    """
    if not site:
        return ""
    s = site.strip()
    # если пришёл URL — берём netloc
    if s.startswith(("http://", "https://")):
        try:
            p = urlparse(s)
            host = (p.netloc or "").strip().rstrip("/")
            return quote(host, safe="")
        except Exception:
            pass
    # не URL: иногда могут прислать "www.host/" или "host/path"
    if "://" in s:
        try:
            host = urlparse(s).netloc
            s = host or s
        except Exception:
            pass
    # уберём хвостовой слеш и всё после первого слеша (если вдруг есть путь)
    s = s.split("/", 1)[0].strip()
    # percent-encode на всякий случай (но для ASCII-доменов это noop)
    return quote(s, safe="")


from urllib.parse import quote, urlparse
import httpx

def _extract_host(site: str) -> str:
    """Берём чистый host/netloc из URL/домена и обрезаем путь/слеши."""
    s = (site or "").strip()
    if not s:
        return ""
    if s.startswith(("http://", "https://")):
        try:
            p = urlparse(s)
            host = (p.netloc or "").strip().rstrip("/")
            return host
        except Exception:
            pass
    # если случайно пришёл 'host/path'
    s = s.split("/", 1)[0].strip().rstrip("/")
    return s

def _build_variants_for_site(site: str) -> list[str]:
    """
    Формируем варианты значения {site} для внешнего сервиса:
    1) host (как есть)
    2) если нет префикса 'www.' — попробуем 'www.' + host
    Оба варианта percent-encode.
    """
    host = _extract_host(site)
    variants = []
    if host:
        variants.append(quote(host, safe=""))
        if not host.lower().startswith("www."):
            variants.append(quote(f"www.{host}", safe=""))
    return variants

async def _call_external_analyze(site: str, inn: str) -> tuple[bool, str]:
    """
    POST {BASE}/v1/analyze/by-site/{site} с телом {} и JSON-заголовками.
    Пробуем 1–2 варианта (host; при необходимости www.host).
    """
    base = (
        getattr(settings, "AI_ANALYZE_BASE", None)
        or getattr(settings, "ANALYZE_BASE", None)
        or "http://37.221.125.221:8123"
    ).rstrip("/")

    timeout_s = int(getattr(settings, "AI_ANALYZE_TIMEOUT", 60))
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    variants = _build_variants_for_site(site)
    if not variants:
        return False, "external analyze: empty site"

    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True, http2=False) as client:
        last_err = None
        for v in variants:
            url = f"{base}/v1/analyze/by-site/{v}"
            try:
                # ⬇️ тело запроса ровно '{}'
                resp = await client.post(url, json={}, headers=headers)
                if 200 <= resp.status_code < 300:
                    log.info("Analyze OK: %s (%s)", url, resp.status_code)
                    return True, f"external analyze ok @ {url}"
                text = (resp.text or "")[:300]
                log.warning("Analyze non-2xx: %s %s %s", url, resp.status_code, text)
                # если не 2xx — попробуем следующий вариант (если он есть)
                last_err = f"{resp.status_code}"
            except Exception as e:
                # покажем тип исключения для ясности
                etype = type(e).__name__
                log.warning("Analyze request failed: %s [%s] %s", url, etype, e)
                last_err = f"{etype}: {e}"

    return False, f"external analyze failed ({last_err})"

@router.post(
    "/ai-analyzer",
    response_model=AiAnalyzerResponse,
    summary="AI-анализ компании по ИНН (POST)",
)
async def post_ai_analyzer(payload: AiAnalyzerRequest) -> AiAnalyzerResponse:
    """
    Алгоритм POST:
    1) Сначала достаём домены/данные из БД (чтобы знать, что дергать).
    2) Если нашли хотя бы один сайт — вызываем внешний сервис /v1/analyze/by-site/{site} POST и ждём 200.
    3) После ответа — повторно читаем из БД и отдаём канонический ответ.
       (если внешний сервис упал — всё равно отдаём ответ из БД, добавив примечание).
    """
    inn = payload.inn

    # 1) первичное чтение — чтобы взять сайт
    first = await analyze_company_by_inn(inn)
    sites = first.get("sites") or []
    site = None
    if sites:
        site = sites[0]
    elif first.get("domain1"):
        site = first["domain1"]
    elif first.get("domain2"):
        site = first["domain2"]

    external_note = None
    if site:
        ok, msg = await _call_external_analyze(site, inn)
        external_note = msg
    else:
        external_note = "no site to analyze"

    # 3) повторное чтение — уже «как GET»
    second = await analyze_company_by_inn(inn)
    note = second.get("note")
    if external_note:
        note = f"{note} | {external_note}" if note else external_note

    return _canonical_response(**{**second, "note": note})


@router.get(
    "/{inn}/ai-analyzer",
    response_model=AiAnalyzerResponse,
    summary="AI-анализ компании по ИНН (GET)",
)
async def get_ai_analyzer(inn: str) -> AiAnalyzerResponse:
    """
    GET: ничего не вызываем, просто отдаём, что есть в БД.
    """
    if not inn.isdigit() or len(inn) not in (10, 12):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="ИНН должен содержать 10 или 12 цифр",
        )
    result = await analyze_company_by_inn(inn)
    return _canonical_response(**result)
