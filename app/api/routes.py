from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db.bitrix import get_bitrix_session
from app.models.bitrix import DaDataResult
from app.schemas.org import (
    IngestRequest,
    OrgResponse,
    CompanySummaryOut,
    CompanyCard,
    OrgExtendedResponse,
)
from app.services.mapping import extract_inn, map_summary_from_dadata
from app.services.dadata_client import find_party_by_inn
from app.repo.bitrix_repo import (
    replace_dadata_raw,
    upsert_company_summary,
    get_last_raw,
)
from app.db.parsing import push_clients_request  # запись во вторую БД (мягко)

log = logging.getLogger("dadata-bitrix")
router = APIRouter(prefix="/v1")


# ==============
#   Helpers
# ==============

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
    """{short_name_opf} | {revenue/1e6} млн | DaData, c префиксом статуса при необходимости."""
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

def _card_from_summary_dict(d: dict) -> CompanyCard:
    """Собрать CompanyCard из словаря summary (как map_summary_from_dadata)."""
    card = CompanyCard(
        address=d.get("address"),
        geo_lat=d.get("geo_lat"),
        geo_lon=d.get("geo_lon"),
        status=d.get("status"),
        employee_count=d.get("employee_count"),
        main_okved=d.get("main_okved"),
        income=d.get("income"),
        revenue=d.get("revenue"),
        phones=(list(d.get("phones") or []) or None),
        emails=(list(d.get("emails") or []) or None),
    )
    card.company_title = _build_company_title(d.get("short_name_opf"), d.get("revenue"), d.get("status"))
    card.production_address_2024 = _build_production_address(d.get("address"), d.get("geo_lat"), d.get("geo_lon"))
    return card

def _card_from_model(m: DaDataResult) -> CompanyCard:
    """Собрать CompanyCard из ORM-модели (когда берём fallback из БД)."""
    card = CompanyCard(
        address=m.address,
        geo_lat=m.geo_lat,
        geo_lon=m.geo_lon,
        status=m.status,
        employee_count=m.employee_count,
        main_okved=m.main_okved,
        income=float(m.income) if m.income is not None else None,
        revenue=float(m.revenue) if m.revenue is not None else None,
        phones=list(m.phones) if m.phones else None,
        emails=list(m.emails) if m.emails else None,
    )
    card.company_title = _build_company_title(m.short_name_opf, card.revenue, card.status)
    card.production_address_2024 = _build_production_address(m.address, m.geo_lat, m.geo_lon)
    return card

async def _return_from_db_or_503(inn: str, session: AsyncSession, err_msg: str) -> OrgExtendedResponse:
    """
    Возвратить данные из БД, если они есть; иначе 503.
    Используется как fallback, когда DaData не отвечает.
    """
    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)

    if not summary and not raw_payload:
        # Нет сети и нет кэша — временная недоступность
        raise HTTPException(status_code=503, detail=f"{err_msg}; локальных данных по ИНН нет")

    card = _card_from_model(summary) if summary else None
    return OrgExtendedResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
        card=card,
    )


# ==========================================
#   Ingest готового payload (как было)
# ==========================================
@router.post("/bitrix/ingest", response_model=OrgResponse)
async def ingest_dadata(
    body: IngestRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Принимает JSON с DaData (один объект), сохраняет raw+summary.
    Параллельно пытается записать короткую строку во вторую БД (best-effort).
    """
    payload = body.payload
    inn = extract_inn(payload)
    if not inn:
        raise HTTPException(status_code=400, detail="Не удалось определить ИНН в payload")

    summary_dict = map_summary_from_dadata(payload)
    summary_dict.setdefault("inn", inn)

    try:
        await replace_dadata_raw(session, inn=inn, payload=payload)
        await upsert_company_summary(session, data=summary_dict)
        await session.commit()
    except Exception:
        await session.rollback()
        raise HTTPException(status_code=500, detail="Ошибка сохранения данных")

    # best-effort запись во вторую БД
    try:
        ok = await push_clients_request(summary_dict, domain=getattr(body, "domain", None))
        if ok:
            log.info("parsing_data.clients_requests: запись добавлена (INGEST), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (INGEST), ИНН %s: %s", inn, e)

    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    return OrgResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
    )


# ==========================================
#   Обычный lookup (как было)
# ==========================================
class LookupRequest(BaseModel):
    inn: str
    domain: Optional[str] = None  # опционально


@router.post("/lookup", response_model=OrgResponse)
async def lookup_post(
    dto: LookupRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    inn = (dto.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup (POST) для ИНН %s: %s", inn, e)
        resp = await _return_from_db_or_503(inn, session, "DaData недоступна")
        return OrgResponse(summary=resp.summary, raw_last=resp.raw_last)

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

    try:
        ok = await push_clients_request(summary_dict, domain=dto.domain)
        if ok:
            log.info("parsing_data.clients_requests: запись добавлена (LOOKUP POST), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (LOOKUP POST), ИНН %s: %s", inn, e)

    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    return OrgResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
    )


@router.get("/lookup/{inn}", response_model=OrgResponse)
async def lookup_get(
    inn: str,
    domain: Optional[str] = Query(None, description="Адрес сайта (домен), необязательно"),
    session: AsyncSession = Depends(get_bitrix_session),
):
    inn = (inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup (GET) для ИНН %s: %s", inn, e)
        resp = await _return_from_db_or_503(inn, session, "DaData недоступна")
        return OrgResponse(summary=resp.summary, raw_last=resp.raw_last)

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

    try:
        ok = await push_clients_request(summary_dict, domain=domain)
        if ok:
            log.info("parsing_data.clients_requests: запись добавлена (LOOKUP GET), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (LOOKUP GET), ИНН %s: %s", inn, e)

    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    return OrgResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
    )


# ==========================================
#   НОВЫЕ РОУТЫ: lookup + карточка
# ==========================================

class LookupCardRequest(BaseModel):
    inn: str
    domain: Optional[str] = None  # опционально; пишется во 2ю БД как domain_1


@router.post("/lookup/card", response_model=OrgExtendedResponse)
async def lookup_card_post(
    dto: LookupCardRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Всё как /v1/lookup, но дополнительно возвращаем 'card':
    - company_title: `{short_name_opf} | {revenue/1e6} млн | DaData`, с префиксом !!!STATUS!!!
    - production_address_2024: `{Address}|{geo_lat};{geo_lon}|21`
    а также адрес/координаты/статус/ССЧ/ОКВЭД/финансы/контакты.
    """
    inn = (dto.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup/card (POST) для ИНН %s: %s", inn, e)
        return await _return_from_db_or_503(inn, session, "DaData недоступна")

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

    # best-effort запись во вторую БД
    try:
        ok = await push_clients_request(summary_dict, domain=dto.domain)
        if ok:
            log.info("parsing_data.clients_requests: запись добавлена (LOOKUP CARD POST), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (LOOKUP CARD POST), ИНН %s: %s", inn, e)

    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    card = _card_from_summary_dict(summary_dict)

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
    """GET-вариант с тем же результатом, что и POST /v1/lookup/card."""
    inn = (inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup/card (GET) для ИНН %s: %s", inn, e)
        return await _return_from_db_or_503(inn, session, "DaData недоступна")

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

    # best-effort запись во вторую БД
    try:
        ok = await push_clients_request(summary_dict, domain=domain)
        if ok:
            log.info("parsing_data.clients_requests: запись добавлена (LOOKUP CARD GET), ИНН %s", inn)
    except Exception as e:
        log.warning("parsing_data.clients_requests: ошибка записи (LOOKUP CARD GET), ИНН %s: %s", inn, e)

    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)
    card = _card_from_summary_dict(summary_dict)

    return OrgExtendedResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
        card=card,
    )


# ===============================
#   Получить сохранённые данные
# ===============================
@router.get("/org/{inn}", response_model=OrgResponse)
async def get_org(
    inn: str,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """Быстрый доступ к сохранённым данным: сводка + последний raw по ИНН."""
    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)

    if not summary and not raw_payload:
        raise HTTPException(status_code=404, detail="Данные по ИНН не найдены")

    return OrgResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
    )
