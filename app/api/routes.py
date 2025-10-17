from __future__ import annotations

import logging
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Body, Path
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.bitrix import get_bitrix_session
from app.db.pp719 import pp719_has_inn

# parsing_data (зеркало)
from app.db.parsing import push_clients_request as push_clients_request_pd

# POSTGRES (основное хранилище)
from app.db.parsing_mirror import push_clients_request_pg
from app.db.postgres import get_postgres_engine

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
from app.services.parse_site import (
    ParseSiteRequest,
    ParseSiteResponse,
    schedule_parse_site_background,
    run_parse_site,
)
from app.schemas.equipment_selection import EquipmentSelectionResponse
from app.services.equipment_selection import (
    EquipmentSelectionNotFound,
    compute_equipment_selection,
    resolve_client_request_id,
)

log = logging.getLogger("api.routes")
router = APIRouter(prefix="/v1")
ib_match_router = APIRouter(prefix="/ib-match", tags=["IB Matching"])
parse_site_router = APIRouter(prefix="/parse-site", tags=["Parse Site"])
equipment_selection_router = APIRouter(
    prefix="/equipment-selection", tags=["Equipment Selection"]
)


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


async def _assign_ib_matches(*, client_id: int, reembed_if_exists: bool) -> IbMatchResponse:
    log.info(
        "ib-match: POST /ib-match requested (client_id=%s, reembed_if_exists=%s)",
        client_id,
        reembed_if_exists,
    )
    try:
        result = await assign_ib_matches(
            client_id=client_id,
            reembed_if_exists=reembed_if_exists,
        )
    except IbMatchServiceError as exc:
        log.warning(
            "ib-match: POST /ib-match failed (client_id=%s): %s",
            client_id,
            exc,
        )
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    log.info("ib-match: POST /ib-match succeeded (client_id=%s)", client_id)
    return IbMatchResponse.model_validate(result)


async def _assign_ib_matches_by_inn(
    *,
    inn: str,
    reembed_if_exists: bool,
    request_label: str,
) -> IbMatchResponse:
    log.info(
        "ib-match: %s requested (inn=%s, reembed_if_exists=%s)",
        request_label,
        inn,
        reembed_if_exists,
    )
    try:
        result = await assign_ib_matches_by_inn(
            inn=inn,
            reembed_if_exists=reembed_if_exists,
        )
    except IbMatchServiceError as exc:
        log.warning(
            "ib-match: %s failed (inn=%s): %s",
            request_label,
            inn,
            exc,
        )
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    log.info(
        "ib-match: %s succeeded (inn=%s, client_id=%s)",
        request_label,
        inn,
        result.get("client_id"),
    )
    return IbMatchResponse.model_validate(result)


@ib_match_router.post("", response_model=IbMatchResponse)
async def ib_match(payload: IbMatchRequest = Body(...)) -> IbMatchResponse:
    """Присваивает соответствия товаров и оборудования из справочников IB."""

    return await _assign_ib_matches(
        client_id=payload.client_id,
        reembed_if_exists=payload.reembed_if_exists,
    )


@ib_match_router.post("/by-inn", response_model=IbMatchResponse)
async def ib_match_by_inn(payload: IbMatchInnRequest = Body(...)) -> IbMatchResponse:
    """Присваивает соответствия, определяя клиента по ИНН."""

    return await _assign_ib_matches_by_inn(
        inn=payload.inn,
        reembed_if_exists=payload.reembed_if_exists,
        request_label="POST /ib-match/by-inn",
    )


@ib_match_router.get("/by-inn", response_model=IbMatchResponse)
async def ib_match_by_inn_get(
    inn: str = Query(..., min_length=4, max_length=20, description="ИНН клиента"),
    reembed_if_exists: bool = Query(
        False,
        description="Если true, заново генерирует эмбеддинги даже при наличии text_vector",
    ),
) -> IbMatchResponse:
    """GET-вариант сопоставления по ИНН (только чтение параметра inn)."""

    return await _assign_ib_matches_by_inn(
        inn=inn,
        reembed_if_exists=reembed_if_exists,
        request_label="GET /ib-match/by-inn",
    )


router.include_router(ib_match_router)


@parse_site_router.post(
    "",
    response_model=ParseSiteResponse,
    summary="Парсинг главной страницы домена и сохранение в pars_site",
)
async def parse_site(
    payload: ParseSiteRequest = Body(...),
    session: AsyncSession = Depends(get_bitrix_session),
):
    payload_dump = payload.model_dump()
    log.info("parse-site POST: получен payload %s", payload_dump)
    response = await run_parse_site(payload, session)
    log.info("parse-site POST: завершено для ИНН %s → %s", payload.inn, response.model_dump())
    return response


@parse_site_router.get(
    "/{inn}",
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
    parse_domain: str | None = Query(
        None,
        description="Одиночный домен или URL (например, 'uniconf.ru' или 'https://uniconf.ru').",
    ),
    parse_domains: list[str] | None = Query(
        None,
        description="Список доменов/URL для парсинга. Переданные значения дополняются доменами из БД.",
    ),
    parse_emails: list[str] | None = Query(
        None,
        description="Список email-адресов. Домены из них будут использованы как кандидаты для парсинга.",
    ),
    company_name: str | None = Query(
        None,
        description="Название компании; если не задано — подтянем из своей БД (DaDataResult)",
    ),
    client_domain_1: str | None = Query(
        None,
        description="clients_requests.domain_1; если не задано — 'www.{первый_домен}'",
    ),
    pars_site_domain_1: str | None = Query(
        None,
        description="pars_site.domain_1; если не задано — домен без 'www.'",
    ),
    url_override: str | None = Query(
        None,
        description="pars_site.url; если не задано — главная страница",
    ),
    save_client_request: bool = Query(
        True,
        description="Создавать запись в clients_requests (по умолчанию — да)",
    ),
    session: AsyncSession = Depends(get_bitrix_session),
):
    log.info("parse-site GET: получен запрос по ИНН %s", inn)
    payload = ParseSiteRequest(
        inn=inn,
        parse_domain=parse_domain,
        parse_domains=parse_domains or None,
        parse_emails=parse_emails or None,
        company_name=company_name,
        client_domain_1=client_domain_1,
        pars_site_domain_1=pars_site_domain_1,
        url_override=url_override,
        save_client_request=save_client_request,
    )
    log.info("parse-site GET: подготовлен payload %s", payload.model_dump())
    response = await run_parse_site(payload, session)
    log.info("parse-site GET: завершено для ИНН %s → %s", inn, response.model_dump())
    return response


router.include_router(parse_site_router)


@equipment_selection_router.get(
    "",
    response_model=EquipmentSelectionResponse,
    summary="Расчёт оборудования по clients_requests.id",
)
async def get_equipment_selection(
    client_request_id: int = Query(..., ge=1, description="ID записи в clients_requests"),
):
    log.info(
        "equipment-selection GET: starting computation for clients_requests.id=%s",
        client_request_id,
    )
    engine = get_postgres_engine()
    if engine is None:
        log.error("equipment-selection GET: postgres engine is not configured")
        raise HTTPException(status_code=503, detail="Postgres engine is not configured")
    async with engine.connect() as conn:
        try:
            result = await compute_equipment_selection(conn, client_request_id)
        except EquipmentSelectionNotFound as exc:  # pragma: no cover - network/db required
            log.warning(
                "equipment-selection GET: selection not found for clients_requests.id=%s",
                client_request_id,
            )
            raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info(
        "equipment-selection GET: finished computation for clients_requests.id=%s",
        client_request_id,
    )
    log.debug(
        "equipment-selection GET: response payload %s",
        result.model_dump(),
    )
    return result


@equipment_selection_router.get(
    "/by-inn/{inn}",
    response_model=EquipmentSelectionResponse,
    summary="Расчёт оборудования по последней записи клиента с указанным ИНН",
)
async def get_equipment_selection_by_inn(
    inn: str = Path(..., min_length=4, max_length=20, regex=r"^\d+$"),
):
    log.info("equipment-selection GET/by-inn: starting for INN %s", inn)
    engine = get_postgres_engine()
    if engine is None:
        log.error("equipment-selection GET/by-inn: postgres engine is not configured")
        raise HTTPException(status_code=503, detail="Postgres engine is not configured")
    async with engine.connect() as conn:
        client_request_id = await resolve_client_request_id(conn, inn)
        if client_request_id is None:
            log.warning(
                "equipment-selection GET/by-inn: clients_requests not found for INN %s",
                inn,
            )
            raise HTTPException(status_code=404, detail=f"clients_requests for INN {inn} not found")
        try:
            result = await compute_equipment_selection(conn, client_request_id)
        except EquipmentSelectionNotFound as exc:  # pragma: no cover - network/db required
            log.warning(
                "equipment-selection GET/by-inn: selection not found for INN %s (id=%s)",
                inn,
                client_request_id,
            )
            raise HTTPException(status_code=404, detail=str(exc)) from exc
    log.info(
        "equipment-selection GET/by-inn: finished for INN %s (clients_requests.id=%s)",
        inn,
        client_request_id,
    )
    log.debug(
        "equipment-selection GET/by-inn: response payload %s",
        result.model_dump(),
    )
    return result


router.include_router(equipment_selection_router)
