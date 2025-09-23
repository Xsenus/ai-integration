# app/api/routes.py
from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.bitrix import get_bitrix_session
from app.db.pp719 import pp719_has_inn

# parsing_data (зеркало)
from app.db.parsing import (
    clients_requests_exists,
    get_clients_request_id as get_clients_request_id_pd,
    push_clients_request as push_clients_request_pd,
    table_exists as table_exists_pd,
    pars_site_insert_chunks as pars_site_insert_chunks_pd,
)

# POSTGRES (основное хранилище)
from app.db.parsing_mirror import (
    push_clients_request_pg,
    get_clients_request_id_pg,
    pars_site_insert_chunks_pg,
)

from app.models.bitrix import DaDataResult
from app.repo.bitrix_repo import (
    get_last_raw,
    replace_dadata_raw,
    upsert_company_summary,
)
from app.schemas.org import (
    CompanyCard,
    CompanySummaryOut,
    OrgExtendedResponse,
)
from app.services.dadata_client import find_party_by_inn
from app.services.mapping import map_summary_from_dadata
from app.services.scrape import fetch_and_chunk, FetchError

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
        code = (item.get("code") or "").strip()
        name = (item.get("name") or "").strip()
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


# ==========================================
#   Парсинг главной страницы домена → pars_site
# ==========================================

class ParseSiteRequest(BaseModel):
    company_name: str = Field(..., description="Название компании для clients_requests.company_name")
    inn: str = Field(..., min_length=4, max_length=20, description="ИНН для clients_requests.inn")
    client_domain_1: str = Field(..., description="Домен клиента (как в clients_requests.domain_1), напр. 'www.uniconf.ru'")
    parse_domain: str = Field(..., description="Домен или URL для запроса, напр. 'uniconf.ru' или 'https://uniconf.ru'")
    pars_site_domain_1: str | None = Field(None, description="Как писать в pars_site.domain_1; по умолчанию — нормализованный домен")
    save_client_request: bool = Field(True, description="Создавать запись в clients_requests")
    url_override: str | None = Field(None, description="Чем заполнить pars_site.url; если пусто — главная страница")


class ParseSiteResponse(BaseModel):
    company_id: int
    domain_1: str
    url: str
    chunks_inserted: int


@router.post("/parse-site", response_model=ParseSiteResponse, summary="Парсинг главной страницы домена и сохранение в pars_site")
async def parse_site(payload: ParseSiteRequest = Body(...)):
    if not settings.SCRAPERAPI_KEY:
        raise HTTPException(status_code=400, detail="SCRAPERAPI_KEY is not configured on server")

    # 1) upsert в POSTGRES (основная БД)
    minimal_summary = {"short_name": payload.company_name, "inn": payload.inn}
    if payload.save_client_request:
        try:
            await push_clients_request_pg(minimal_summary, domain=payload.client_domain_1)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"PG upsert clients_requests failed: {e}") from e

    # 1.1) Получаем company_id из POSTGRES
    company_id_pg = await get_clients_request_id_pg(payload.inn, payload.client_domain_1)
    if not company_id_pg:
        raise HTTPException(status_code=500, detail="PG: не удалось определить company_id после upsert по ИНН")

    # 2) Тянем и режем HTML
    try:
        home_url, chunks, normalized_domain = await fetch_and_chunk(payload.parse_domain)
    except FetchError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    domain_for_pars = payload.pars_site_domain_1 or normalized_domain
    url_for_pars = payload.url_override or home_url
    chunks_payload = [{"start": i, "end": i, "text": t} for i, t in enumerate(chunks or []) if t]

    # 3) Вставляем чанки в POSTGRES
    inserted_pg = 0
    if chunks_payload:
        inserted_pg = await pars_site_insert_chunks_pg(
            company_id=company_id_pg,
            domain_1=domain_for_pars,
            url=url_for_pars,
            chunks=chunks_payload,
        )

    # 4) Зеркалим в parsing_data (best-effort; свой company_id)
    try:
        if payload.save_client_request:
            await push_clients_request_pd(minimal_summary, domain=payload.client_domain_1)
            company_id_pd = await get_clients_request_id_pd(payload.inn, payload.client_domain_1)
        else:
            company_id_pd = await get_clients_request_id_pd(payload.inn, payload.client_domain_1)

        if company_id_pd and chunks_payload:
            await pars_site_insert_chunks_pd(
                company_id=company_id_pd,
                domain_1=domain_for_pars,
                url=url_for_pars,
                chunks=chunks_payload,
            )
    except Exception as e:
        log.warning("mirror parsing_data failed for INN=%s: %s", payload.inn, e)

    # Возвращаем данные по основному хранилищу (POSTGRES)
    return ParseSiteResponse(
        company_id=company_id_pg,
        domain_1=domain_for_pars,
        url=url_for_pars,
        chunks_inserted=inserted_pg,
    )
