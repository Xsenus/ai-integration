from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Iterable, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Body, Path
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import urlparse as _urlparse

from app.config import settings
from app.db.bitrix import get_bitrix_session, bitrix_session
from app.db.pp719 import pp719_has_inn

# parsing_data (зеркало)
from app.db.parsing import (
    clients_requests_exists,
    get_clients_request_id as get_clients_request_id_pd,
    get_domains_by_inn as get_domains_by_inn_pd,
    push_clients_request as push_clients_request_pd,
    table_exists as table_exists_pd,
    pars_site_insert_chunks as pars_site_insert_chunks_pd,
)

# POSTGRES (основное хранилище)
from app.db.parsing_mirror import (
    push_clients_request_pg,
    get_clients_request_id_pg,
    pars_site_insert_chunks_pg,
    get_domains_by_inn_pg,
    get_ib_clients_domains_pg,
)

from app.models.bitrix import DaDataResult
from app.repo.bitrix_repo import (
    get_last_raw,
    replace_dadata_raw,
    upsert_company_summary,
)
from app.schemas.ib_match import IbMatchInnRequest, IbMatchRequest, IbMatchResponse
from app.schemas.org import (
    CompanyCard,
    CompanySummaryOut,
    OrgExtendedResponse,
)
from app.services.dadata_client import find_party_by_inn
from app.services.ib_match import (
    assign_ib_matches,
    assign_ib_matches_by_inn,
    IbMatchServiceError,
)
from app.services.mapping import map_summary_from_dadata
from app.services.scrape import fetch_and_chunk, FetchError, to_home_url

log = logging.getLogger("api.routes")
router = APIRouter(prefix="/v1")


# =========================
#        Helpers
# =========================

def _mln_text(revenue: Optional[float]) -> str:
    """revenue → 'N млн' (целое, округление)."""
    if revenue is None:
        return "0 млн"
    try:
        mln = int(round(float(revenue) / 1_000_000))
        return f"{mln} млн"
    except Exception:
        return "0 млн"


def _build_company_title(short_name_opf: Optional[str], revenue: Optional[float], status: Optional[str]) -> str:
    """{short_name_opf} | {revenue/1e6} млн | DaData, с префиксом статуса при необходимости."""
    base_name = (short_name_opf or "").strip()
    rev_txt = _mln_text(revenue)
    title = f"{base_name} | {rev_txt} | DaData".strip()

    st = (status or "").upper()
    if st in {"LIQUIDATING", "BANKRUPT", "LIQUIDATED"}:
        title = f"!!!{st}!!! {title}"
    return title


def _build_production_address(address: Optional[str], lat: Optional[float], lon: Optional[float]) -> str:
    """{Address}|{geo_lat};{geo_lon}|21 — пустые части допускаются."""
    addr = (address or "").strip()
    lat_s = "" if lat is None else str(lat)
    lon_s = "" if lon is None else str(lon)
    return f"{addr}|{lat_s};{lon_s}|21"


def _okved_to_text(item: object) -> Optional[str]:
    """
    item может быть строкой ('47.11 Розничная торговля ...') или dict{'code','name'}.
    Возвращает 'code name' или саму строку, если это уже текст.
    """
    if item is None:
        return None
    if isinstance(item, str):
        return item.strip() or None
    if isinstance(item, dict):
        code = (item.get("code") or "").strip()  # type: ignore[attr-defined]
        name = (item.get("name") or "").strip()  # type: ignore[attr-defined]
        if code and name:
            return f"{code} {name}"
        return code or name or None
    return None


def _extract_main_code(main_okved: Optional[str]) -> Optional[str]:
    """
    Пытаемся выделить код из main_okved.
    Если там только код — возвращаем его.
    Если там 'код название' — возвращаем первую 'слово-похожую-на-код' часть.
    """
    if not main_okved:
        return None
    s = str(main_okved).strip()
    if all(ch.isdigit() or ch in {'.'} for ch in s):
        return s
    return s.split()[0]


def _fill_vtors_from_okveds(okveds: object, main_okved: Optional[str]) -> dict:
    """
    Собирает okved_vtor_1..7 из списка okveds, исключая главный код (если можем его распознать).
    okveds может быть: list[str] | list[dict] | None
    """
    items: list[str] = []
    if isinstance(okveds, list):
        for it in okveds:
            txt = _okved_to_text(it)
            if txt:
                items.append(txt)

    main_code = _extract_main_code(main_okved)
    if main_code:
        items = [x for x in items if not x.startswith(main_code)]

    vtors = {}
    for i, val in enumerate(items[:7], start=1):
        vtors[f"okved_vtor_{i}"] = val
    return vtors


def _pick_hist(d: dict, base: str, idx: int):
    """
    Берет значение из словаря d по ключам base_idx или base-idx.
    Например: base='revenue', idx=1 -> 'revenue_1' или 'revenue-1'
    """
    return d.get(f"{base}_{idx}", d.get(f"{base}-{idx}"))


def _as_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _card_from_summary_dict(d: dict) -> CompanyCard:
    """Собрать CompanyCard из словаря summary (как map_summary_from_dadata)."""
    card = CompanyCard(
        inn=d.get("inn"),
        short_name=d.get("short_name"),
        short_name_opf=d.get("short_name_opf"),

        management_full_name=d.get("management_full_name"),
        management_surname_n_p=d.get("management_surname_n_p"),
        management_surname=d.get("management_surname"),
        management_name=d.get("management_name"),
        management_patronymic=d.get("management_patronymic"),
        management_post=d.get("management_post"),

        branch_count=d.get("branch_count"),

        address=d.get("address"),
        geo_lat=d.get("geo_lat"),
        geo_lon=d.get("geo_lon"),
        status=d.get("status"),
        employee_count=d.get("employee_count"),

        main_okved=d.get("main_okved"),
        okved_main=d.get("main_okved"),

        year=d.get("year"),
        income=d.get("income"),
        revenue=d.get("revenue"),
        smb_type=d.get("smb_type"),
        smb_category=d.get("smb_category"),
        smb_issue_date=d.get("smb_issue_date"),
        phones=(list(d.get("phones") or []) or None),
        emails=(list(d.get("emails") or []) or None),
    )

    # Исторические поля: поддерживаем snake_case и dash-case
    card.revenue_1 = _as_float(_pick_hist(d, "revenue", 1))
    card.revenue_2 = _as_float(_pick_hist(d, "revenue", 2))
    card.revenue_3 = _as_float(_pick_hist(d, "revenue", 3))

    card.income_1 = _as_float(_pick_hist(d, "income", 1))
    card.income_2 = _as_float(_pick_hist(d, "income", 2))
    card.income_3 = _as_float(_pick_hist(d, "income", 3))

    ec1 = _pick_hist(d, "employee_count", 1)
    ec2 = _pick_hist(d, "employee_count", 2)
    ec3 = _pick_hist(d, "employee_count", 3)
    card.employee_count_1 = int(ec1) if ec1 is not None else None
    card.employee_count_2 = int(ec2) if ec2 is not None else None
    card.employee_count_3 = int(ec3) if ec3 is not None else None

    vtors = _fill_vtors_from_okveds(d.get("okveds"), d.get("main_okved"))
    for k, v in vtors.items():
        setattr(card, k, v)

    card.company_title = _build_company_title(card.short_name_opf, card.revenue, card.status)
    card.production_address_2024 = _build_production_address(card.address, card.geo_lat, card.geo_lon)
    return card


def _card_from_model(m: DaDataResult) -> CompanyCard:
    """Собрать CompanyCard из ORM-модели (fallback из своей БД)."""
    card = CompanyCard(
        inn=m.inn,
        short_name=m.short_name,
        short_name_opf=m.short_name_opf,

        management_full_name=m.management_full_name,
        management_surname_n_p=m.management_surname_n_p,
        management_surname=m.management_surname,
        management_name=m.management_name,
        management_patronymic=m.management_patronymic,
        management_post=m.management_post,

        branch_count=m.branch_count,

        address=m.address,
        geo_lat=m.geo_lat,
        geo_lon=m.geo_lon,
        status=m.status,
        employee_count=m.employee_count,

        main_okved=m.main_okved,
        okved_main=m.main_okved,

        year=m.year,
        income=float(m.income) if m.income is not None else None,
        revenue=float(m.revenue) if m.revenue is not None else None,
        smb_type=m.smb_type,
        smb_category=m.smb_category,
        smb_issue_date=m.smb_issue_date,
        phones=list(m.phones) if m.phones else None,
        emails=list(m.emails) if m.emails else None,

        # Исторические значения (Decimal → float / int)
        revenue_1=_as_float(m.revenue_1),
        revenue_2=_as_float(m.revenue_2),
        revenue_3=_as_float(m.revenue_3),

        income_1=_as_float(m.income_1),
        income_2=_as_float(m.income_2),
        income_3=_as_float(m.income_3),

        employee_count_1=int(m.employee_count_1) if m.employee_count_1 is not None else None,
        employee_count_2=int(m.employee_count_2) if m.employee_count_2 is not None else None,
        employee_count_3=int(m.employee_count_3) if m.employee_count_3 is not None else None,
    )

    vtors = _fill_vtors_from_okveds(m.okveds, m.main_okved)
    for k, v in vtors.items():
        setattr(card, k, v)

    card.company_title = _build_company_title(card.short_name_opf, card.revenue, card.status)
    card.production_address_2024 = _build_production_address(card.address, card.geo_lat, card.geo_lon)
    return card


# ==============================
#     Расширенный LOOKUP
# ==============================

class LookupCardRequest(BaseModel):
    inn: str
    domain: Optional[str] = None  # опционально; пишется в clients_requests.domain_1 (с www.)


@router.post("/lookup/card", response_model=OrgExtendedResponse)
async def lookup_card_post(
    dto: LookupCardRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Как обычный lookup, но дополнительно возвращаем 'card'
    и пишем clients_requests в обе БД (PG — основная, parsing_data — зеркало).
    """
    inn = (dto.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup/card (POST) для ИНН %s: %s", inn, e)
        # fallback из БД, если есть
        summary = await session.get(DaDataResult, inn)
        raw_payload = await get_last_raw(session, inn)
        if not summary and not raw_payload:
            raise HTTPException(status_code=503, detail="DaData недоступна; локальных данных по ИНН нет")
        return OrgExtendedResponse(
            summary=CompanySummaryOut.model_validate(summary) if summary else None,
            raw_last=raw_payload,
            card=_card_from_model(summary) if summary else None,
        )

    if not suggestion:
        raise HTTPException(status_code=404, detail="Организация не найдена в DaData")

    summary_dict = map_summary_from_dadata(suggestion)
    summary_dict.setdefault("inn", inn)

    try:
        await replace_dadata_raw(session, inn=inn, payload=suggestion)
        await upsert_company_summary(session, data=summary_dict)
        await session.commit()
    except Exception:
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка сохранения данных")

    # --- запись в POSTGRES (основная) ---
    try:
        ok_pg = await push_clients_request_pg(summary_dict, domain=dto.domain)
        if ok_pg:
            log.info("PG clients_requests: запись добавлена (LOOKUP CARD POST), ИНН %s", inn)
    except Exception as e:
        log.warning("PG clients_requests: ошибка записи (LOOKUP CARD POST), ИНН %s: %s", inn, e)

    # --- зеркало в parsing_data (best-effort) ---
    try:
        ok_pd = await push_clients_request_pd(summary_dict, domain=dto.domain)
        if ok_pd:
            log.info("parsing_data.clients_requests: запись добавлена (LOOKUP CARD POST), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (LOOKUP CARD POST), ИНН %s: %s", inn, e)

    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    card = _card_from_summary_dict(summary_dict)

    try:
        if card and card.inn and await pp719_has_inn(card.inn):
            base = card.short_name_opf or card.short_name or ""
            if card.company_title and base:
                card.company_title = card.company_title.replace(base, f"{base} (ПП719)", 1)
    except Exception as e:
        log.warning("pp719 check failed for %s: %s", inn, e)

    schedule_parse_site_background(
        inn=inn,
        parse_domain=dto.domain,
        company_name=summary_dict.get("short_name"),
        save_client_request=False,
        reason="lookup/card-post",
    )

    return OrgExtendedResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
        card=card,
    )


@router.get("/lookup/{inn}/card", response_model=OrgExtendedResponse)
async def lookup_card_get(
    inn: str,
    domain: Optional[str] = Query(None, description="Адрес сайта (домен), необязательно"),
    session: AsyncSession = Depends(get_bitrix_session),
):
    """Сначала читаем из своей БД; если нет — идём в DaData, сохраняем и возвращаем. Пишем в обе БД."""
    inn = (inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    # 1) Пытаемся отдать из своей БД (без обращения к DaData)
    summary = await session.get(DaDataResult, inn)
    if summary:
        raw_payload = await get_last_raw(session, inn)
        summary_dict = CompanySummaryOut.model_validate(summary).model_dump()
        card = _card_from_summary_dict(summary_dict)

        try:
            if card and card.inn and await pp719_has_inn(card.inn):
                base = card.short_name_opf or card.short_name or ""
                if card.company_title and base:
                    card.company_title = card.company_title.replace(base, f"{base} (ПП719)", 1)
        except Exception as e:
            log.warning("pp719 check failed for %s: %s", inn, e)

        return OrgExtendedResponse(
            summary=CompanySummaryOut.model_validate(summary),
            raw_last=raw_payload,
            card=card,
        )

    # 2) Если в БД нет — fallback на DaData
    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup/card (GET) для ИНН %s: %s", inn, e)
        raise HTTPException(status_code=503, detail="DaData недоступна; локальных данных по ИНН нет")

    if not suggestion:
        raise HTTPException(status_code=404, detail="Организация не найдена в DaData")

    summary_dict = map_summary_from_dadata(suggestion)
    summary_dict.setdefault("inn", inn)

    try:
        await replace_dadata_raw(session, inn=inn, payload=suggestion)
        await upsert_company_summary(session, data=summary_dict)
        await session.commit()
    except Exception:
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка сохранения данных")

    # --- запись в POSTGRES (основная) ---
    try:
        ok_pg = await push_clients_request_pg(summary_dict, domain=domain)
        if ok_pg:
            log.info("PG clients_requests: запись добавлена (LOOKUP CARD GET), ИНН %s", inn)
    except Exception as e:
        log.warning("PG clients_requests: ошибка записи (LOOKUP CARD GET), ИНН %s: %s", inn, e)

    # --- зеркало в parsing_data ---
    try:
        ok_pd = await push_clients_request_pd(summary_dict, domain=domain)
        if ok_pd:
            log.info("parsing_data.clients_requests: запись добавлена (LOOKUP CARD GET), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (LOOKUP CARD GET), ИНН %s: %s", inn, e)

    # Итоговый ответ
    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    card = _card_from_summary_dict(summary_dict)

    try:
        if card and card.inn and await pp719_has_inn(card.inn):
            base = card.short_name_opf or card.short_name or ""
            if card.company_title and base:
                card.company_title = card.company_title.replace(base, f"{base} (ПП719)", 1)
    except Exception as e:
        log.warning("pp719 check failed for %s: %s", inn, e)

    return OrgExtendedResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
        card=card,
    )


# ==========================
#      IB matching route
# ==========================


@router.post("/ib-match", response_model=IbMatchResponse)
async def ib_match(payload: IbMatchRequest = Body(...)) -> IbMatchResponse:
    """Присваивает соответствия товаров и оборудования из справочников IB."""

    log.info(
        "ib-match: POST /ib-match requested (client_id=%s, reembed_if_exists=%s)",
        payload.client_id,
        payload.reembed_if_exists,
    )
    try:
        result = await assign_ib_matches(
            client_id=payload.client_id,
            reembed_if_exists=payload.reembed_if_exists,
        )
    except IbMatchServiceError as exc:
        log.warning(
            "ib-match: POST /ib-match failed (client_id=%s): %s",
            payload.client_id,
            exc,
        )
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    log.info(
        "ib-match: POST /ib-match succeeded (client_id=%s)", payload.client_id
    )
    return IbMatchResponse.model_validate(result)


@router.post("/ib-match/by-inn", response_model=IbMatchResponse)
async def ib_match_by_inn(payload: IbMatchInnRequest = Body(...)) -> IbMatchResponse:
    """Присваивает соответствия, определяя клиента по ИНН."""

    log.info(
        "ib-match: POST /ib-match/by-inn requested (inn=%s, reembed_if_exists=%s)",
        payload.inn,
        payload.reembed_if_exists,
    )
    try:
        result = await assign_ib_matches_by_inn(
            inn=payload.inn,
            reembed_if_exists=payload.reembed_if_exists,
        )
    except IbMatchServiceError as exc:
        log.warning(
            "ib-match: POST /ib-match/by-inn failed (inn=%s): %s",
            payload.inn,
            exc,
        )
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    log.info(
        "ib-match: POST /ib-match/by-inn succeeded (inn=%s, client_id=%s)",
        payload.inn,
        result.get("client_id"),
    )
    return IbMatchResponse.model_validate(result)


@router.get("/ib-match/by-inn", response_model=IbMatchResponse)
async def ib_match_by_inn_get(
    inn: str = Query(..., min_length=4, max_length=20, description="ИНН клиента"),
) -> IbMatchResponse:
    """GET-вариант сопоставления по ИНН (только чтение параметра inn)."""

    log.info("ib-match: GET /ib-match/by-inn requested (inn=%s)", inn)
    try:
        result = await assign_ib_matches_by_inn(
            inn=inn,
            reembed_if_exists=False,
        )
    except IbMatchServiceError as exc:
        log.warning(
            "ib-match: GET /ib-match/by-inn failed (inn=%s): %s",
            inn,
            exc,
        )
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    log.info(
        "ib-match: GET /ib-match/by-inn succeeded (inn=%s, client_id=%s)",
        inn,
        result.get("client_id"),
    )
    return IbMatchResponse.model_validate(result)


# ==========================================
#   Парсинг главной страницы домена → pars_site
# ==========================================

_DOMAIN_IN_TEXT_RE = re.compile(
    r"(?:https?://|http://|ftp://)?(?:www\.)?([a-z0-9.-]+\.[a-z]{2,})",
    re.IGNORECASE,
)


def _normalize_domain_candidate(domain: str) -> Optional[str]:
    try:
        _, normalized = _normalize_and_split_domain(domain)
        return normalized
    except Exception:
        return None


def _extract_domains_from_value(value: object) -> Iterable[str]:
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        domains: list[str] = []
        for item in value:
            domains.extend(_extract_domains_from_value(item))
        return domains

    if isinstance(value, dict):
        domains: list[str] = []
        for item in value.values():
            domains.extend(_extract_domains_from_value(item))
        return domains

    text = str(value).strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
    except Exception:  # noqa: BLE001
        parsed = None

    if isinstance(parsed, (list, dict)):
        return list(_extract_domains_from_value(parsed))

    matches = [m.group(1) for m in _DOMAIN_IN_TEXT_RE.finditer(text)]
    return matches


def _normalize_domains(values: Iterable[object]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        for raw in _extract_domains_from_value(value):
            norm = _normalize_domain_candidate(raw)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            result.append(norm)
    return result


async def _collect_domains_by_inn(inn: str, session: AsyncSession) -> list[str]:
    """Возвращает уникальный список доменов для ИНН из разных источников."""

    candidates: list[object] = []

    log.info("parse-site: начинаем сбор доменов для ИНН %s", inn)

    try:
        domains_pg = await get_domains_by_inn_pg(inn)
        if domains_pg:
            log.info(
                "parse-site: найдены домены в POSTGRES.clients_requests по ИНН %s (%s шт.) → %s",
                inn,
                len(domains_pg),
                domains_pg,
            )
            candidates.extend(domains_pg)
        else:
            log.info("parse-site: в POSTGRES.clients_requests нет доменов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения POSTGRES.clients_requests для ИНН %s: %s", inn, e)

    try:
        domains_pd = await get_domains_by_inn_pd(inn)
        if domains_pd:
            log.info(
                "parse-site: найдены домены в parsing_data.clients_requests по ИНН %s (%s шт.) → %s",
                inn,
                len(domains_pd),
                domains_pd,
            )
            candidates.extend(domains_pd)
        else:
            log.info("parse-site: в parsing_data.clients_requests нет доменов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения parsing_data.clients_requests для ИНН %s: %s", inn, e)

    try:
        ib_domains = await get_ib_clients_domains_pg(inn)
        if ib_domains:
            log.info(
                "parse-site: найдены домены в POSTGRES.ib_clients по ИНН %s (%s шт.) → %s",
                inn,
                len(ib_domains),
                ib_domains,
            )
            candidates.extend(ib_domains)
        else:
            log.info("parse-site: в POSTGRES.ib_clients нет доменов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения POSTGRES.ib_clients для ИНН %s: %s", inn, e)

    try:
        res = await session.execute(select(DaDataResult.web_sites).where(DaDataResult.inn == inn))
        web_sites = res.scalar_one_or_none()
        if web_sites:
            log.info(
                "parse-site: найдены сайты в bitrix_data.dadata_result по ИНН %s → %s",
                inn,
                web_sites,
            )
            candidates.append(web_sites)
        else:
            log.info("parse-site: в bitrix_data.dadata_result нет сайтов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения bitrix_data.dadata_result.web_sites для ИНН %s: %s", inn, e)

    normalized = _normalize_domains(candidates)
    log.info(
        "parse-site: итоговый список доменов по ИНН %s (%s шт.) → %s",
        inn,
        len(normalized),
        normalized,
    )
    return normalized


class ParseSiteRequest(BaseModel):
    # Теперь parse_domain можно не передавать: попытаемся определить по ИНН
    inn: str = Field(..., min_length=4, max_length=20, description="ИНН для clients_requests.inn")
    parse_domain: str | None = Field(
        None,
        description="Одиночный домен или URL (например, 'uniconf.ru' или 'https://uniconf.ru')."
    )
    parse_domains: list[str] | None = Field(
        None,
        description="Список доменов/URL для парсинга. Переданные значения дополняются доменами из БД."
    )

    # Опциональные override'ы
    company_name: str | None = Field(None, description="Название компании; если не задано — подтянем из своей БД (DaDataResult)")
    client_domain_1: str | None = Field(None, description="clients_requests.domain_1; если не задано — 'www.{первый_домен}'")
    pars_site_domain_1: str | None = Field(None, description="pars_site.domain_1; если не задано — домен без 'www.'")
    url_override: str | None = Field(None, description="pars_site.url; если не задано — главная страница")
    save_client_request: bool = Field(True, description="Создавать запись в clients_requests (по умолчанию — да)")


class ParsedSiteResult(BaseModel):
    requested_domain: str
    used_domain: str | None
    url: str | None
    chunks_inserted: int
    success: bool
    error: str | None = None


class ParseSiteResponse(BaseModel):
    company_id: int
    domain_1: str | None
    domain_2: str | None
    url: str | None
    chunks_inserted: int
    results: list[ParsedSiteResult]


async def _run_parse_site_background(
    *,
    inn: str,
    parse_domain: str | None,
    company_name: str | None,
    save_client_request: bool,
    reason: str,
) -> None:
    """Выполнить parse-site в отдельной задаче."""

    payload_data: dict[str, object] = {
        "inn": inn,
        "save_client_request": save_client_request,
    }
    if parse_domain:
        payload_data["parse_domain"] = parse_domain
    if company_name:
        payload_data["company_name"] = company_name

    try:
        async with bitrix_session() as session:
            payload = ParseSiteRequest(**payload_data)
            await _parse_site_impl(payload, session)
    except HTTPException as e:
        log.info(
            "parse-site background skipped (%s): %s (inn=%s, domain=%s)",
            reason,
            e.detail,
            inn,
            parse_domain,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "parse-site background failed (%s) for inn=%s, domain=%s",
            reason,
            inn,
            parse_domain,
        )


def schedule_parse_site_background(
    *,
    inn: str,
    parse_domain: str | None,
    company_name: str | None = None,
    save_client_request: bool = False,
    reason: str,
) -> None:
    """Запускает parse-site в фоне, не блокируя ответ API."""

    inn_clean = (inn or "").strip()
    if not inn_clean:
        log.debug("parse-site background not scheduled: empty inn (%s).", reason)
        return

    domain_clean = (parse_domain or "").strip() or None
    name_clean = (company_name or "").strip() or None

    async def runner() -> None:
        await _run_parse_site_background(
            inn=inn_clean,
            parse_domain=domain_clean,
            company_name=name_clean,
            save_client_request=save_client_request,
            reason=reason,
        )

    try:
        asyncio.create_task(runner(), name=f"parse-site:{inn_clean}")
    except Exception:  # noqa: BLE001
        log.exception(
            "parse-site background scheduling failed (%s) for inn=%s, domain=%s",
            reason,
            inn_clean,
            domain_clean,
        )


async def _parse_site_impl(payload: ParseSiteRequest, session: AsyncSession) -> ParseSiteResponse:
    if not settings.SCRAPERAPI_KEY:
        raise HTTPException(status_code=400, detail="SCRAPERAPI_KEY is not configured on server")

    inn = (payload.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    log.info(
        "parse-site: старт обработки (ИНН=%s, save_client_request=%s)",
        inn,
        payload.save_client_request,
    )

    manual_domains: list[object] = []
    if payload.parse_domain:
        manual_domains.append(payload.parse_domain)
    if payload.parse_domains:
        manual_domains.extend(payload.parse_domains)

    if manual_domains:
        log.info("parse-site: переданные вручную домены → %s", manual_domains)
    else:
        log.info("parse-site: ручные домены не переданы")

    normalized_manual = _normalize_domains(manual_domains)
    log.info("parse-site: нормализованные ручные домены → %s", normalized_manual)
    domains_to_process: list[str]
    if normalized_manual:
        domains_to_process = list(normalized_manual)
        seen = set(domains_to_process)
        extra = await _collect_domains_by_inn(inn, session)
        for dom in extra:
            if dom not in seen:
                domains_to_process.append(dom)
                seen.add(dom)
        log.info(
            "parse-site: итоговые домены после объединения с БД (%s шт.) → %s",
            len(domains_to_process),
            domains_to_process,
        )
    else:
        domains_to_process = await _collect_domains_by_inn(inn, session)
        log.info(
            "parse-site: домены определены только из БД (%s шт.) → %s",
            len(domains_to_process),
            domains_to_process,
        )

    if not domains_to_process:
        log.info("parse-site: домены не найдены для ИНН %s, возвращаем ошибку", inn)
        raise HTTPException(
            status_code=400,
            detail="Не удалось определить домен: передайте 'parse_domain'/'parse_domains' или добавьте данные в БД",
        )

    company_name = (payload.company_name or "").strip()
    if not company_name:
        persisted = await session.get(DaDataResult, inn)
        if persisted:
            company_name = (
                (persisted.short_name_opf or persisted.short_name or "").strip()
            ) or None
            log.info(
                "parse-site: название компании найдено в bitrix_data.dadata_result → %s",
                company_name,
            )
        else:
            log.info("parse-site: название компании в bitrix_data.dadata_result не найдено")
    else:
        log.info("parse-site: название компании передано в запросе → %s", company_name)

    client_domain_override = (payload.client_domain_1 or "").strip()
    if client_domain_override:
        log.info("parse-site: override client_domain_1=%s", client_domain_override)
        normalized_client = _normalize_domain_candidate(client_domain_override)
        if not normalized_client:
            raise HTTPException(status_code=400, detail="Некорректный client_domain_1")
        if normalized_client in domains_to_process:
            domains_to_process.remove(normalized_client)
        domains_to_process.insert(0, normalized_client)
        log.info("parse-site: домен override установлен первым → %s", normalized_client)

    primary_domain_norm = domains_to_process[0]
    secondary_domain_norm = domains_to_process[1] if len(domains_to_process) > 1 else None

    planned_client_domains = [primary_domain_norm]
    if secondary_domain_norm:
        planned_client_domains.append(secondary_domain_norm)
    log.info(
        "parse-site: исходные кандидаты для clients_requests → %s",
        [f"www.{dom}" for dom in planned_client_domains],
    )

    override_domain_for_pars: Optional[str] = None
    if payload.pars_site_domain_1:
        try:
            _, override_domain_for_pars = _normalize_and_split_domain(payload.pars_site_domain_1)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Некорректный pars_site_domain_1: {e}") from e
        if len(domains_to_process) > 1:
            log.info(
                "parse-site: pars_site_domain_1 override игнорируется для части доменов (ИНН %s)",
                inn,
            )
        log.info("parse-site: override pars_site_domain_1=%s", override_domain_for_pars)

    override_url = (payload.url_override or "").strip() or None
    if override_url and len(domains_to_process) > 1:
        log.info(
            "parse-site: url_override игнорируется для части доменов (ИНН %s)",
            inn,
        )
    if override_url:
        log.info("parse-site: override url=%s", override_url)

    company_id_pg = await get_clients_request_id_pg(inn)
    log.info("parse-site: текущий company_id в POSTGRES.clients_requests → %s", company_id_pg)
    if not payload.save_client_request and company_id_pg is None:
        raise HTTPException(
            status_code=400,
            detail="Запись в clients_requests не найдена; разрешите save_client_request, чтобы создать её",
        )

    summary_from_dadata: dict[str, Any] | None = None
    if payload.save_client_request and company_id_pg is None:
        log.info("parse-site: записей в clients_requests нет, запрашиваем DaData по ИНН %s", inn)
        try:
            suggestion = await find_party_by_inn(inn)
        except httpx.HTTPError as e:
            log.warning("DaData недоступна при parse-site для ИНН %s: %s", inn, e)
        else:
            if suggestion:
                summary_from_dadata = map_summary_from_dadata(suggestion)
                summary_from_dadata.setdefault("inn", inn)
                log.info("parse-site: DaData вернула данные для ИНН %s", inn)
                try:
                    await replace_dadata_raw(session, inn=inn, payload=suggestion)
                    await upsert_company_summary(session, data=summary_from_dadata)
                    await session.commit()
                    log.info("parse-site: данные DaData сохранены в bitrix_data")
                except Exception as e:  # noqa: BLE001
                    await session.rollback()
                    log.warning("Не удалось сохранить данные DaData для ИНН %s: %s", inn, e)
            else:
                log.info("DaData не вернула данных по ИНН %s", inn)

    minimal_summary: dict[str, Any]
    if summary_from_dadata is not None:
        minimal_summary = dict(summary_from_dadata)
    else:
        minimal_summary = {"short_name": company_name, "inn": inn}

    minimal_summary.setdefault("inn", inn)
    if company_name:
        minimal_summary.setdefault("short_name", company_name)
    else:
        short_name_from_summary = (minimal_summary.get("short_name") or "").strip()
        company_name = short_name_from_summary or company_name

    log.info("parse-site: итоговый summary для clients_requests → %s", minimal_summary)

    summary_for_client_base = dict(minimal_summary)

    company_id_pd: Optional[int] = None
    mirror_failed_logged = False
    try:
        company_id_pd = await get_clients_request_id_pd(inn)
    except Exception as e:  # noqa: BLE001
        log.warning("mirror parsing_data failed for INN=%s: %s", inn, e)
        mirror_failed_logged = True
        company_id_pd = None
    else:
        log.info(
            "parse-site: текущий company_id в parsing_data.clients_requests → %s",
            company_id_pd,
        )

    clients_request_synced = False
    synced_client_domain_2: Optional[str] = None
    client_domain_lookup: Optional[str] = None
    client_domains_success: list[str] = []

    results: list[ParsedSiteResult] = []
    successes: list[ParsedSiteResult] = []
    total_inserted = 0

    for idx, domain_candidate in enumerate(domains_to_process, start=1):
        log.info(
            "parse-site: начинаем обработку домена %s (%s/%s)",
            domain_candidate,
            idx,
            len(domains_to_process),
        )
        try:
            home_url, chunks, normalized_domain = await fetch_and_chunk(domain_candidate)
            log.info(
                "parse-site: получен контент для %s → %s чанков (url=%s, нормализованный домен=%s)",
                domain_candidate,
                len(chunks),
                home_url,
                normalized_domain,
            )
        except FetchError as e:
            log.warning(
                "parse-site: ошибка парсинга домена %s → %s",
                domain_candidate,
                e,
            )
            results.append(
                ParsedSiteResult(
                    requested_domain=domain_candidate,
                    used_domain=None,
                    url=None,
                    chunks_inserted=0,
                    success=False,
                    error=str(e),
                )
            )
            continue

        domain_for_pars = (
            override_domain_for_pars if override_domain_for_pars and len(domains_to_process) == 1 else normalized_domain
        )
        url_for_pars = override_url if override_url and len(domains_to_process) == 1 else home_url

        if domain_candidate not in client_domains_success:
            client_domains_success.append(domain_candidate)
        current_client_domain_1 = client_domains_success[0]
        current_client_domain_2 = client_domains_success[1] if len(client_domains_success) > 1 else None

        chunks_payload = [
            {"start": i, "end": i, "text": text}
            for i, text in enumerate(chunks or [])
            if text
        ]
        log.info(
            "parse-site: подготовлено чанков к вставке для %s → %s шт.",
            domain_candidate,
            len(chunks_payload),
        )

        inserted_pg = 0
        if chunks_payload:
            if payload.save_client_request:
                summary_for_client = dict(summary_for_client_base)
                if not clients_request_synced:
                    log.info(
                        "parse-site: выполняем upsert clients_requests (POSTGRES/parsing_data) для ИНН %s",
                        inn,
                    )
                    try:
                        await push_clients_request_pg(
                            summary_for_client,
                            domain=current_client_domain_1,
                            domain_secondary=current_client_domain_2,
                        )
                    except Exception as e:
                        raise HTTPException(
                            status_code=500,
                            detail=f"PG upsert clients_requests failed: {e}",
                        ) from e

                    client_domain_lookup = f"www.{current_client_domain_1}" if current_client_domain_1 else None
                    company_id_pg = await get_clients_request_id_pg(inn, client_domain_lookup)
                    if not company_id_pg:
                        company_id_pg = await get_clients_request_id_pg(inn)
                    if not company_id_pg:
                        raise HTTPException(
                            status_code=500,
                            detail="PG: не удалось определить company_id после upsert по ИНН",
                        )
                    log.info(
                        "parse-site: после upsert получен company_id в POSTGRES=%s",
                        company_id_pg,
                    )

                    try:
                        await push_clients_request_pd(
                            summary_for_client,
                            domain=current_client_domain_1,
                            domain_secondary=current_client_domain_2,
                        )
                        company_id_pd = await get_clients_request_id_pd(inn, client_domain_lookup)
                    except Exception as e:  # noqa: BLE001
                        if not mirror_failed_logged:
                            log.warning("mirror parsing_data failed for INN=%s: %s", inn, e)
                            mirror_failed_logged = True
                    else:
                        log.info(
                            "parse-site: upsert clients_requests в parsing_data завершен, company_id=%s",
                            company_id_pd,
                        )

                    clients_request_synced = True
                    synced_client_domain_2 = current_client_domain_2
                    log.info(
                        "parse-site: синхронизация clients_requests выполнена (domain_1=%s, domain_2=%s)",
                        f"www.{current_client_domain_1}" if current_client_domain_1 else None,
                        f"www.{current_client_domain_2}" if current_client_domain_2 else None,
                    )
                elif current_client_domain_2 and current_client_domain_2 != synced_client_domain_2:
                    log.info(
                        "parse-site: обновляем domain_2 для clients_requests → %s",
                        f"www.{current_client_domain_2}",
                    )
                    try:
                        await push_clients_request_pg(
                            summary_for_client,
                            domain=None,
                            domain_secondary=current_client_domain_2,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "parse-site: обновление POSTGRES.clients_requests domain_2 не удалось для ИНН %s: %s",
                            inn,
                            e,
                        )
                    else:
                        synced_client_domain_2 = current_client_domain_2
                        log.info(
                            "parse-site: domain_2 обновлён в POSTGRES.clients_requests → %s",
                            f"www.{current_client_domain_2}",
                        )

                    if company_id_pd:
                        try:
                            await push_clients_request_pd(
                                summary_for_client,
                                domain=None,
                                domain_secondary=current_client_domain_2,
                            )
                        except Exception as e:  # noqa: BLE001
                            if not mirror_failed_logged:
                                log.warning("mirror parsing_data failed для обновления domain_2 (ИНН %s): %s", inn, e)
                                mirror_failed_logged = True
                        else:
                            log.info(
                                "parse-site: domain_2 обновлён в parsing_data.clients_requests → %s",
                                f"www.{current_client_domain_2}",
                            )

            if company_id_pg is None:
                raise HTTPException(
                    status_code=500,
                    detail="PG: не удалось определить company_id для pars_site",
                )

            log.info(
                "parse-site: сохраняем чанки в POSTGRES.pars_site (company_id=%s, домен=%s, url=%s)",
                company_id_pg,
                domain_for_pars,
                url_for_pars,
            )
            inserted_pg = await pars_site_insert_chunks_pg(
                company_id=company_id_pg,
                domain_1=domain_for_pars,
                url=url_for_pars,
                chunks=chunks_payload,
            )
            log.info(
                "parse-site: вставлено чанков в POSTGRES.pars_site для %s → %s",
                domain_for_pars,
                inserted_pg,
            )
            total_inserted += inserted_pg

            if company_id_pd:
                try:
                    log.info(
                        "parse-site: сохраняем чанки в parsing_data.pars_site (company_id=%s, домен=%s, url=%s)",
                        company_id_pd,
                        domain_for_pars,
                        url_for_pars,
                    )
                    await pars_site_insert_chunks_pd(
                        company_id=company_id_pd,
                        domain_1=domain_for_pars,
                        url=url_for_pars,
                        chunks=chunks_payload,
                    )
                except Exception as e:  # noqa: BLE001
                    if not mirror_failed_logged:
                        log.warning("mirror parsing_data failed for INN=%s: %s", inn, e)
                        mirror_failed_logged = True
                else:
                    log.info(
                        "parse-site: чанки записаны в parsing_data.pars_site для %s",
                        domain_for_pars,
                    )

        result = ParsedSiteResult(
            requested_domain=domain_candidate,
            used_domain=domain_for_pars,
            url=url_for_pars,
            chunks_inserted=inserted_pg,
            success=True,
            error=None,
        )
        results.append(result)
        successes.append(result)
        log.info(
            "parse-site: домен %s обработан успешно (чанков=%s)",
            domain_for_pars,
            inserted_pg,
        )

    if not successes:
        errors = "; ".join(filter(None, (r.error for r in results))) or "Парсинг не удался"
        log.warning("parse-site: все домены завершились ошибкой для ИНН %s → %s", inn, errors)
        raise HTTPException(status_code=502, detail=errors)

    final_domain_1 = client_domains_success[0] if client_domains_success else None
    final_domain_2 = client_domains_success[1] if len(client_domains_success) > 1 else None

    log.info(
        "parse-site: финальные домены для clients_requests → domain_1=%s, domain_2=%s",
        f"www.{final_domain_1}" if final_domain_1 else None,
        f"www.{final_domain_2}" if final_domain_2 else None,
    )

    primary = successes[0]
    log.info(
        "parse-site: успешно обработано доменов %s/%s, всего вставлено чанков %s",
        len(successes),
        len(results),
        total_inserted,
    )
    return ParseSiteResponse(
        company_id=company_id_pg,
        domain_1=f"www.{final_domain_1}" if final_domain_1 else None,
        domain_2=f"www.{final_domain_2}" if final_domain_2 else None,
        url=primary.url,
        chunks_inserted=total_inserted,
        results=results,
    )


def _normalize_and_split_domain(domain_or_url: str) -> tuple[str, str]:
    """
    Возвращает (home_url, normalized_domain без www).
    """
    home_url = to_home_url(domain_or_url)
    normalized_domain = _urlparse(home_url).netloc.replace("www.", "")
    return home_url, normalized_domain


@router.post("/parse-site", response_model=ParseSiteResponse, summary="Парсинг главной страницы домена и сохранение в pars_site")
async def parse_site(
    payload: ParseSiteRequest = Body(...),
    session: AsyncSession = Depends(get_bitrix_session),
):
    payload_dump = payload.model_dump()
    log.info("parse-site POST: получен payload %s", payload_dump)
    response = await _parse_site_impl(payload, session)
    log.info("parse-site POST: завершено для ИНН %s → %s", payload.inn, response.model_dump())
    return response


@router.get(
    "/parse-site/{inn}",
    response_model=ParseSiteResponse,
    summary="Парсинг главной страницы по ИНН с автоопределением домена",
)
async def parse_site_by_inn(
    inn: str = Path(
        ...,
        min_length=4,
        max_length=20,
        regex=r"^\d+$",
        description="ИНН компании, для которой нужно подтянуть домен",
    ),
    session: AsyncSession = Depends(get_bitrix_session),
):
    log.info("parse-site GET: получен запрос по ИНН %s", inn)
    payload = ParseSiteRequest(inn=inn)
    response = await _parse_site_impl(payload, session)
    log.info("parse-site GET: завершено для ИНН %s → %s", inn, response.model_dump())
    return response
