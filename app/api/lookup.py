from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.bitrix import get_bitrix_session
from app.db.parsing import push_clients_request as push_clients_request_pd
from app.db.parsing_mirror import push_clients_request_pg
from app.db.pp719 import pp719_has_inn
from app.models.bitrix import DaDataResult
from app.repo.bitrix_repo import get_last_raw, replace_dadata_raw, upsert_company_summary
from app.schemas.ai_analyzer import (
    AiAnalyzerResponse,
    AiBlock,
    AiEquipment,
    AiProdclass,
    AiProduct,
    CompanyBlock,
)
from app.schemas.org import CompanyCard, CompanySummaryOut, OrgExtendedResponse
from app.services.dadata_client import find_party_by_inn
from app.services.mapping import map_summary_from_dadata
from app.services.parse_site import schedule_parse_site_background
from app.services.ai_analyzer import analyze_company_by_inn

log = logging.getLogger("api.lookup")
router = APIRouter(prefix="/v1/lookup", tags=["lookup"])


class LookupCardRequest(BaseModel):
    inn: str
    domain: Optional[str] = None


def _mln_text(revenue: Optional[float]) -> str:
    if revenue is None:
        return "0 млн"
    try:
        mln = int(round(float(revenue) / 1_000_000))
        return f"{mln} млн"
    except Exception:
        return "0 млн"


def _build_company_title(short_name_opf: Optional[str], revenue: Optional[float], status: Optional[str]) -> str:
    base_name = (short_name_opf or "").strip()
    rev_txt = _mln_text(revenue)
    title = f"{base_name} | {rev_txt} | DaData".strip()

    st = (status or "").upper()
    if st in {"LIQUIDATING", "BANKRUPT", "LIQUIDATED"}:
        title = f"!!!{st}!!! {title}"
    return title


def _build_production_address(address: Optional[str], lat: Optional[float], lon: Optional[float]) -> str:
    addr = (address or "").strip()
    lat_s = "" if lat is None else str(lat)
    lon_s = "" if lon is None else str(lon)
    return f"{addr}|{lat_s};{lon_s}|21"


def _okved_to_text(item: object) -> Optional[str]:
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
    if not main_okved:
        return None
    s = str(main_okved).strip()
    if all(ch.isdigit() or ch in {'.'} for ch in s):
        return s
    return s.split()[0]


def _fill_vtors_from_okveds(okveds: object, main_okved: Optional[str]) -> dict:
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
    return d.get(f"{base}_{idx}", d.get(f"{base}-{idx}"))


def _as_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _card_from_model(m: DaDataResult) -> CompanyCard:
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
        income=_as_float(m.income),
        revenue=_as_float(m.revenue),
        smb_type=m.smb_type,
        smb_category=m.smb_category,
        smb_issue_date=m.smb_issue_date,
        phones=list(m.phones) if m.phones else None,
        emails=list(m.emails) if m.emails else None,
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


def _card_from_summary_dict(d: dict) -> CompanyCard:
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


def _ai_analyzer_response_from_payload(
    inn: str,
    payload: Mapping[str, Any] | None,
) -> AiAnalyzerResponse:
    data = dict(payload or {})

    company = CompanyBlock(
        domain1=
        data.get("domain1")
        or data.get("domain1_description"),
        domain2=
        data.get("domain2")
        or data.get("domain2_description"),
    )

    products = [
        AiProduct.model_validate(item)
        for item in (data.get("products") or [])
        if item is not None
    ]
    equipment = [
        AiEquipment.model_validate(item)
        for item in (data.get("equipment") or [])
        if item is not None
    ]

    prodclass_payload = data.get("prodclass")
    prodclass = None
    if prodclass_payload:
        prodclass = AiProdclass.model_validate(prodclass_payload)

    ai_block = AiBlock(
        industry=data.get("industry") or data.get("industry_label"),
        prodclass=prodclass,
        sites=list(data.get("sites") or []),
        products=products,
        equipment=equipment,
        utp=data.get("utp"),
        letter=data.get("letter"),
    )

    return AiAnalyzerResponse(
        ok=True,
        inn=inn,
        company=company,
        ai=ai_block,
        note=data.get("note"),
    )


async def _load_local_summary(
    session: AsyncSession, inn: str
) -> tuple[DaDataResult | None, CompanySummaryOut | None, dict | None, dict | None]:
    summary_row = await session.get(DaDataResult, inn)
    if summary_row is None:
        return None, None, None, None
    summary = CompanySummaryOut.model_validate(summary_row)
    raw_payload = await get_last_raw(session, inn)
    return summary_row, summary, summary.model_dump(), raw_payload


async def _lookup_card_internal(
    inn: str,
    *,
    session: AsyncSession,
    domain: str | None,
    force_refresh: bool,
    schedule_parse: bool,
    parse_reason: str,
) -> OrgExtendedResponse:
    normalized_inn = (inn or "").strip()
    if not normalized_inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    summary_row, summary_model, summary_dict, raw_payload = await _load_local_summary(
        session, normalized_inn
    )
    refreshed = False

    if force_refresh or summary_model is None:
        try:
            suggestion = await find_party_by_inn(normalized_inn)
        except httpx.HTTPError as exc:
            log.warning(
                "DaData недоступна при lookup/card (force=%s) для ИНН %s: %s",
                force_refresh,
                normalized_inn,
                exc,
            )
            if summary_model is None:
                raise HTTPException(
                    status_code=503,
                    detail="DaData недоступна; локальных данных по ИНН нет",
                ) from exc
        else:
            if not suggestion:
                raise HTTPException(status_code=404, detail="Организация не найдена в DaData")

            summary_dict = map_summary_from_dadata(suggestion)
            summary_dict.setdefault("inn", normalized_inn)

            try:
                await replace_dadata_raw(session, inn=normalized_inn, payload=suggestion)
                await upsert_company_summary(session, data=summary_dict)
                await session.commit()
            except Exception:
                await session.rollback()
                raise HTTPException(status_code=500, detail="Ошибка сохранения данных")

            refreshed = True
            summary_row, summary_model, summary_dict, raw_payload = await _load_local_summary(
                session, normalized_inn
            )

    if summary_dict is None:
        raise HTTPException(status_code=404, detail="Организация не найдена в локальном хранилище")

    if summary_row is not None:
        card = _card_from_model(summary_row)
    else:
        card = _card_from_summary_dict(summary_dict)

    try:
        if card and card.inn and await pp719_has_inn(card.inn):
            base = card.short_name_opf or card.short_name or ""
            if card.company_title and base:
                card.company_title = card.company_title.replace(base, f"{base} (ПП719)", 1)
    except Exception as exc:
        log.warning("pp719 check failed for %s: %s", normalized_inn, exc)

    try:
        ok_pg = await push_clients_request_pg(summary_dict, domain=domain)
        if ok_pg:
            log.info(
                "PG clients_requests: запись синхронизирована (LOOKUP CARD, refreshed=%s), ИНН %s",
                refreshed,
                normalized_inn,
            )
    except Exception as exc:
        log.warning("PG clients_requests: ошибка записи (LOOKUP CARD), ИНН %s: %s", normalized_inn, exc)

    try:
        ok_pd = await push_clients_request_pd(summary_dict, domain=domain)
        if ok_pd:
            log.info(
                "parsing_data.clients_requests: запись синхронизирована (LOOKUP CARD, refreshed=%s), ИНН %s",
                refreshed,
                normalized_inn,
            )
    except Exception as exc:
        log.warning(
            "parsing_data.clients_requests: ошибка записи (LOOKUP CARD), ИНН %s: %s",
            normalized_inn,
            exc,
        )

    if schedule_parse:
        schedule_parse_site_background(
            inn=normalized_inn,
            parse_domain=domain,
            company_name=summary_dict.get("short_name"),
            save_client_request=False,
            reason=parse_reason,
        )

    summary_model = summary_model or CompanySummaryOut.model_validate(summary_dict)
    return OrgExtendedResponse(
        summary=summary_model,
        raw_last=raw_payload,
        card=card,
    )


@router.post("/card", response_model=OrgExtendedResponse)
async def lookup_card_post(
    dto: LookupCardRequest,
    session: AsyncSession = Depends(get_bitrix_session),
) -> OrgExtendedResponse:
    return await _lookup_card_internal(
        dto.inn,
        session=session,
        domain=dto.domain,
        force_refresh=True,
        schedule_parse=True,
        parse_reason="lookup/card-post",
    )


@router.get("/{inn}/card", response_model=OrgExtendedResponse)
async def lookup_card_get(
    inn: str,
    domain: Optional[str] = Query(None, description="Адрес сайта (домен), необязательно"),
    session: AsyncSession = Depends(get_bitrix_session),
) -> OrgExtendedResponse:
    return await _lookup_card_internal(
        inn,
        session=session,
        domain=domain,
        force_refresh=False,
        schedule_parse=False,
        parse_reason="lookup/card-get",
    )


@router.get(
    "/{inn}/ai-analyzer",
    response_model=AiAnalyzerResponse,
    summary="AI-анализ компании по ИНН (GET)",
)
async def lookup_ai_analyzer(
    inn: str = Path(
        ...,
        min_length=10,
        max_length=12,
        pattern=r"^\d{10}(\d{2})?$",
        description="ИНН компании (10 или 12 цифр)",
    ),
) -> AiAnalyzerResponse:
    log.info("lookup: ai-analyzer requested (inn=%s)", inn)
    try:
        payload = await analyze_company_by_inn(inn)
    except Exception as exc:  # noqa: BLE001
        log.exception("lookup: ai-analyzer failed (inn=%s)", inn)
        raise HTTPException(status_code=500, detail="Не удалось выполнить AI-анализ") from exc

    response = _ai_analyzer_response_from_payload(inn, payload)
    log.info(
        "lookup: ai-analyzer succeeded (inn=%s, sites=%s, products=%s, equipment=%s)",
        inn,
        len(response.ai.sites),
        len(response.ai.products),
        len(response.ai.equipment),
    )
    return response
