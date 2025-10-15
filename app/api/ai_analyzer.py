from __future__ import annotations

import asyncio
import logging
from typing import Optional
from urllib.parse import quote, urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query, status

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import settings
from app.db.bitrix import bitrix_session
from app.db.postgres import get_postgres_engine
from app.schemas.ai_analyzer import (
    AiAnalyzerRequest,
    AiAnalyzerResponse,
    CompanyBlock,
    AiBlock,
    AiProduct,
    AiEquipment,
    BulkAiAnalyzeLaunchResponse,
)
from app.services.ai_analyzer import analyze_company_by_inn
from app.services.parse_site import ParseSiteRequest, run_parse_site

log = logging.getLogger("api.ai_analyzer")
router = APIRouter(prefix="/v1/lookup", tags=["ai-analyzer"])

_RETRYABLE_STATUS_CODES = {
    408,
    409,
    425,
    429,
    500,
    502,
    503,
    504,
    522,
    524,
}
_MAX_ANALYZE_ATTEMPTS = 3
_RETRY_BACKOFF_BASE = 0.5  # seconds

_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock()
_bulk_analyze_task: asyncio.Task[None] | None = None
_bulk_task_lock = asyncio.Lock()

_CLIENTS_FOR_ANALYZE_SQL = text(
    """
    SELECT DISTINCT ON (inn)
        inn,
        domain_1,
        domain_2
    FROM public.clients_requests
    WHERE NULLIF(TRIM(inn), '') IS NOT NULL
      AND (
            NULLIF(TRIM(domain_1), '') IS NOT NULL
         OR NULLIF(TRIM(domain_2), '') IS NOT NULL
      )
    ORDER BY inn, COALESCE(ended_at, created_at) DESC NULLS LAST, id DESC
    """
)


def _on_bulk_task_done(task: asyncio.Task[None]) -> None:
    """Callback для завершившейся фоновой задачи массового анализа."""

    global _bulk_analyze_task
    try:
        task.result()
    except asyncio.CancelledError:
        log.info("Bulk analyze task cancelled.")
    except Exception:  # noqa: BLE001
        log.exception("Bulk analyze task failed.")
    finally:
        if _bulk_analyze_task is task:
            _bulk_analyze_task = None


async def _collect_companies_for_bulk(
    limit: int | None = None,
    *,
    engine: AsyncEngine | None = None,
) -> list[tuple[str, str]]:
    """Возвращает список (ИНН, домен) для пакетного анализа."""

    eng = engine or get_postgres_engine()
    if eng is None:
        log.warning("Bulk analyze: postgres engine is not configured.")
        return []

    async with eng.connect() as conn:
        res = await conn.execute(_CLIENTS_FOR_ANALYZE_SQL)
        rows = res.mappings().all()

    companies: list[tuple[str, str]] = []
    for row in rows:
        inn = (row.get("inn") or "").strip()
        if not inn:
            continue
        domains = [
            (row.get("domain_1") or "").strip(),
            (row.get("domain_2") or "").strip(),
        ]
        site = next((d for d in domains if d), None)
        if site:
            companies.append((inn, site))

    if limit is not None and limit > 0:
        companies = companies[:limit]

    log.info(
        "Bulk analyze: collected %s companies for processing (limit=%s).",
        len(companies),
        limit,
    )
    return companies


async def _run_bulk_analyze(companies: list[tuple[str, str]]) -> None:
    """Последовательно вызывает внешний сервис анализа для списка компаний."""

    total = len(companies)
    if total == 0:
        log.info("Bulk analyze: nothing to process.")
        return

    log.info("Bulk analyze: started (total=%s).", total)
    for idx, (inn, site) in enumerate(companies, start=1):
        log.info(
            "Bulk analyze: sending %s/%s -> inn=%s, site=%s.",
            idx,
            total,
            inn,
            site,
        )
        await _ensure_site_parsed_for_bulk(inn, site)
        try:
            ok, message = await _call_external_analyze(site, inn)
        except asyncio.CancelledError:
            log.info(
                "Bulk analyze task cancelled while processing inn=%s (site=%s).",
                inn,
                site,
            )
            raise
        except Exception:  # noqa: BLE001
            log.exception(
                "Bulk analyze: unexpected error for inn=%s, site=%s.", inn, site
            )
            continue

        if ok:
            log.info(
                "Bulk analyze: success %s/%s for inn=%s (%s).",
                idx,
                total,
                inn,
                message,
            )
        else:
            log.warning(
                "Bulk analyze: failure %s/%s for inn=%s (%s).",
                idx,
                total,
                inn,
                message,
            )

    log.info("Bulk analyze: completed (total=%s).", total)


async def _ensure_site_parsed_for_bulk(inn: str, site: str) -> None:
    """Готовит pars_site перед запуском внешнего анализа."""

    payload = ParseSiteRequest(
        inn=inn,
        parse_domain=site,
        save_client_request=False,
    )
    try:
        async with bitrix_session() as session:
            await run_parse_site(payload, session)
        log.info(
            "Bulk analyze: pars_site refreshed for inn=%s, site=%s.",
            inn,
            site,
        )
    except HTTPException as e:
        log.info(
            "Bulk analyze: parse-site skipped for inn=%s, site=%s (%s).",
            inn,
            site,
            e.detail,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "Bulk analyze: parse-site failed for inn=%s, site=%s.",
            inn,
            site,
        )


async def _cancel_bulk_task() -> None:
    """Отменяет текущую фоновую задачу (если она активна)."""

    global _bulk_analyze_task
    task = _bulk_analyze_task
    if task is None:
        return

    if not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            log.info("Bulk analyze: task cancellation acknowledged.")
        except Exception:  # noqa: BLE001
            log.exception("Bulk analyze: task raised during cancellation.")

    _bulk_analyze_task = None


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
    variants: list[str] = []
    if host:
        variants.append(quote(host, safe=""))
        if not host.lower().startswith("www."):
            variants.append(quote(f"www.{host}", safe=""))
    # гарантируем сохранение порядка и убираем дубликаты, если host уже начинался с www.
    return list(dict.fromkeys(variants))


async def _get_http_client() -> httpx.AsyncClient:
    """Ленивая инициализация общего httpx.AsyncClient с коннект-пулом."""

    global _http_client
    if _http_client is None:
        async with _http_client_lock:
            if _http_client is None:
                client = httpx.AsyncClient(
                    follow_redirects=True,
                    http2=False,
                    limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
                )
                client.headers.update(
                    {
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "User-Agent": "ai-integration/ai-analyzer",
                    }
                )
                _http_client = client
    assert _http_client is not None
    return _http_client


async def close_ai_analyzer_http_client() -> None:
    """Закрывает переиспользуемый httpx.AsyncClient (на shutdown приложения)."""

    await _cancel_bulk_task()
    global _http_client
    client: httpx.AsyncClient | None
    async with _http_client_lock:
        client = _http_client
        _http_client = None
    if client is not None:
        await client.aclose()


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    timeout: httpx.Timeout,
    inn: str,
) -> tuple[httpx.Response | None, str | None]:
    """POST-запрос с ограниченным числом повторов при транзиентных сбоях."""

    last_error: str | None = None
    for attempt in range(1, _MAX_ANALYZE_ATTEMPTS + 1):
        try:
            resp = await client.post(url, json={}, timeout=timeout)
        except httpx.TimeoutException as exc:
            last_error = f"timeout: {exc}" if str(exc) else "timeout"
            log.warning(
                "Analyze timeout (attempt %s/%s): %s [inn=%s]",
                attempt,
                _MAX_ANALYZE_ATTEMPTS,
                url,
                inn,
            )
        except httpx.RequestError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            log.warning(
                "Analyze request failed (attempt %s/%s): %s [%s] %s [inn=%s]",
                attempt,
                _MAX_ANALYZE_ATTEMPTS,
                url,
                type(exc).__name__,
                exc,
                inn,
            )
        else:
            if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < _MAX_ANALYZE_ATTEMPTS:
                text = (resp.text or "")[:120]
                last_error = f"{resp.status_code}"
                log.warning(
                    "Analyze retryable status %s (attempt %s/%s): %s %s [inn=%s]",
                    resp.status_code,
                    attempt,
                    _MAX_ANALYZE_ATTEMPTS,
                    url,
                    text,
                    inn,
                )
            else:
                return resp, None

        if attempt < _MAX_ANALYZE_ATTEMPTS:
            backoff = min(_RETRY_BACKOFF_BASE * (2 ** (attempt - 1)), 2.0)
            await asyncio.sleep(backoff)

    return None, last_error

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

    timeout_s = max(1, int(getattr(settings, "AI_ANALYZE_TIMEOUT", 60)))
    timeout = httpx.Timeout(
        timeout_s,
        connect=min(float(timeout_s), 10.0),
        read=float(timeout_s),
        write=min(float(timeout_s), 10.0),
    )

    variants = _build_variants_for_site(site)
    if not variants:
        return False, "external analyze: empty site"

    client = await _get_http_client()

    last_err: str | None = None
    for v in variants:
        url = f"{base}/v1/analyze/by-site/{v}"
        resp, err = await _post_with_retry(client, url, timeout=timeout, inn=inn)
        if resp is None:
            last_err = err or "no response"
            continue
        if 200 <= resp.status_code < 300:
            log.info("Analyze OK: %s (%s) [inn=%s]", url, resp.status_code, inn)
            return True, f"external analyze ok @ {url}"
        text = (resp.text or "")[:300]
        log.warning("Analyze non-2xx: %s %s %s [inn=%s]", url, resp.status_code, text, inn)
        last_err = f"{resp.status_code}: {text}" if text else f"{resp.status_code}"

    return False, f"external analyze failed ({last_err})"

@router.post(
    "/ai-analyzer/bulk",
    response_model=BulkAiAnalyzeLaunchResponse,
    summary="Запуск пакетного AI-анализа (POST)",
)
async def launch_bulk_ai_analyzer(
    limit: int | None = Query(
        None,
        ge=1,
        le=10000,
        description="Максимум компаний в запуске (по умолчанию — все)",
    ),
    force: bool = Query(
        False,
        description="Прервать текущий запуск, если он выполняется, и начать заново",
    ),
) -> BulkAiAnalyzeLaunchResponse:
    """Запускает последовательный анализ всех компаний с доменом и ИНН."""

    eng = get_postgres_engine()
    if eng is None:
        detail = "POSTGRES_DATABASE_URL не задан — пакетный анализ отключён"
        log.warning("Bulk analyze: %s", detail)
        return BulkAiAnalyzeLaunchResponse(status="disabled", detail=detail)

    async with _bulk_task_lock:
        global _bulk_analyze_task
        task = _bulk_analyze_task
        if task is not None and not task.done():
            if not force:
                log.info("Bulk analyze: already running, skip new launch (limit=%s).", limit)
                return BulkAiAnalyzeLaunchResponse(
                    status="already_running",
                    detail="Пакетный анализ уже выполняется",
                )
            log.info("Bulk analyze: force restart requested (limit=%s).", limit)
            await _cancel_bulk_task()

        companies = await _collect_companies_for_bulk(limit=limit, engine=eng)
        if not companies:
            detail = "Не найдено компаний с заполненными ИНН и доменом"
            log.info("Bulk analyze: %s (limit=%s).", detail, limit)
            return BulkAiAnalyzeLaunchResponse(status="empty", detail=detail)

        task = asyncio.create_task(
            _run_bulk_analyze(companies),
            name="bulk-ai-analyze",
        )
        task.add_done_callback(_on_bulk_task_done)
        _bulk_analyze_task = task

    log.info(
        "Bulk analyze: scheduled task for %s companies (limit=%s, force=%s).",
        len(companies),
        limit,
        force,
    )
    return BulkAiAnalyzeLaunchResponse(status="started", queued=len(companies))


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
