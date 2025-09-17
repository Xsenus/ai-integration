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
from app.db.parsing import (
    clients_requests_exists,
    get_clients_request_id,
    pars_site_exists,
    push_clients_request,        # best-effort запись во вторую БД
    create_clients_request,      # явное создание clients_requests + возврат id (с fallback)
    insert_pars_site_chunks,     # массовая вставка чанков в pars_site
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
    IngestRequest,
    OrgExtendedResponse,
    OrgResponse,
)
from app.services.dadata_client import find_party_by_inn
from app.services.mapping import extract_inn, map_summary_from_dadata
from app.services.scrape import fetch_and_chunk, FetchError  # NEW

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
    # если строка типа '47.11' — ок
    if all(ch.isdigit() or ch in {'.'} for ch in s):
        return s
    # иначе берём первый токен до пробела/табов/длинных названий
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
        # фильтруем элементы, начинающиеся с кода (на случай 'код название')
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
        okved_main=d.get("main_okved"),  # алиас

        year=d.get("year"),
        income=d.get("income"),
        revenue=d.get("revenue"),
        smb_type=d.get("smb_type"),
        smb_category=d.get("smb_category"),
        smb_issue_date=d.get("smb_issue_date"),
        phones=(list(d.get("phones") or []) or None),
        emails=(list(d.get("emails") or []) or None),
    )
    # дополнительные ОКВЭДы из массива okveds
    vtors = _fill_vtors_from_okveds(d.get("okveds"), d.get("main_okved"))
    for k, v in vtors.items():
        setattr(card, k, v)

    # сгенерированные поля
    card.company_title = _build_company_title(card.short_name_opf, card.revenue, card.status)
    card.production_address_2024 = _build_production_address(card.address, card.geo_lat, card.geo_lon)
    return card


def _card_from_model(m: DaDataResult) -> CompanyCard:
    """Собрать CompanyCard из ORM-модели (когда берём fallback из БД)."""
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
        okved_main=m.main_okved,  # алиас

        year=m.year,
        income=float(m.income) if m.income is not None else None,
        revenue=float(m.revenue) if m.revenue is not None else None,
        smb_type=m.smb_type,
        smb_category=m.smb_category,
        smb_issue_date=m.smb_issue_date,
        phones=list(m.phones) if m.phones else None,
        emails=list(m.emails) if m.emails else None,
    )

    # дополнительные ОКВЭДы из JSONB okveds (list[str|dict] | None)
    vtors = _fill_vtors_from_okveds(m.okveds, m.main_okved)
    for k, v in vtors.items():
        setattr(card, k, v)

    card.company_title = _build_company_title(card.short_name_opf, card.revenue, card.status)
    card.production_address_2024 = _build_production_address(card.address, card.geo_lat, card.geo_lon)
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
#   Ingest готового payload (без обращения к DaData)
# ==========================================
@router.post("/bitrix/ingest", response_model=OrgResponse)
async def ingest_dadata(
    body: IngestRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Принимает JSON с DaData (один объект suggestion/data), сохраняет:
    - raw в dadata_result_full_json (ровно одна строка на ИНН)
    - summary в dadata_result (upsert по inn)
    После успешного сохранения — ПЫТАЕМСЯ записать короткую строку во вторую БД
      (parsing_data.public.clients_requests). Если второй БД/таблицы нет — просто логируем.
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
#   Обычный lookup
# ==========================================

class LookupRequest(BaseModel):
    inn: str
    domain: Optional[str] = None  # опционально; будет записан во вторую БД как domain_1


@router.post("/lookup", response_model=OrgResponse)
async def lookup_post(
    dto: LookupRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Тянем из DaData по ИНН, сохраняем raw+summary, возвращаем сводку и raw.
    Если DaData недоступна, возвращаем данные из БД (если есть), иначе 503.
    После успешного сохранения — ПЫТАЕМСЯ записать короткую строку во вторую БД.
    """
    inn = (dto.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup (POST) для ИНН %s: %s", inn, e)
        resp = await _return_from_db_or_503(inn, session, "DaData недоступна")
        return OrgResponse(summary=resp.summary, raw_last=resp.raw_last)

    # DaData ответила, но организация не найдена — это НЕ повод для fallback
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
    """
    Аналог POST /v1/lookup, но ИНН в пути.
    Fallback: при недоступности DaData отдаём данные из БД (если есть), иначе 503.
    После успешного сохранения — ПЫТАЕМСЯ записать короткую строку во вторую БД.
    """
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

    # best-effort запись во вторую БД
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
#   Расширенный lookup (карточка)
# ==========================================

class LookupCardRequest(BaseModel):
    inn: str
    domain: Optional[str] = None  # опционально; пишется во 2-ю БД как domain_1


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


# ==========================================
#   NEW: Парсинг главной страницы домена → pars_site
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
    # 0) Проверка конфигурации
    if not settings.SCRAPERAPI_KEY:
        raise HTTPException(status_code=400, detail="SCRAPERAPI_KEY is not configured on server")

    # 0.1) Проверяем наличие таблиц. Если их нет — ничего не пишем.
    has_clients = await clients_requests_exists()
    has_pars = await pars_site_exists()
    if not has_clients or not has_pars:
        # Мягкий no-op: возвращаем нули, чтобы фронт/оркестратор понимал, что вставок не было
        return ParseSiteResponse(
            company_id=0,
            domain_1=(payload.pars_site_domain_1 or payload.parse_domain),
            url=(payload.url_override or payload.parse_domain),
            chunks_inserted=0,
        )

    # 1) upsert в clients_requests (обновление при наличии записи, вставка при отсутствии)
    # Составим минимальный summary для push_clients_request
    minimal_summary = {
        "short_name": payload.company_name,
        "inn": payload.inn,
        # остальные поля опциональны; okved/emails и т.п. оставим None
    }
    try:
        await push_clients_request(minimal_summary, domain=payload.client_domain_1)
    except Exception as e:
        # best-effort: если не удалось — считаем, что операцию выполнить нельзя (без company_id нельзя вставлять в pars_site)
        raise HTTPException(status_code=500, detail=f"Не удалось выполнить upsert в clients_requests: {e}") from e

    # 1.1) Получаем company_id по ИНН (и домену, если указан)
    company_id = await get_clients_request_id(payload.inn, payload.client_domain_1)
    if not company_id:
        raise HTTPException(status_code=500, detail="Не удалось определить company_id после upsert по ИНН")

    # 2) Тянем и режем HTML
    try:
        home_url, chunks, normalized_domain = await fetch_and_chunk(payload.parse_domain)
    except FetchError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    if not chunks:
        return ParseSiteResponse(
            company_id=company_id,
            domain_1=(payload.pars_site_domain_1 or normalized_domain),
            url=(payload.url_override or home_url),
            chunks_inserted=0,
        )

    # 3) Вставляем чанки
    domain_for_pars = payload.pars_site_domain_1 or normalized_domain
    url_for_pars = payload.url_override or home_url

    inserted = await insert_pars_site_chunks(
        company_id=company_id,
        domain_1=domain_for_pars,
        url=url_for_pars,
        chunks=chunks,
    )

    return ParseSiteResponse(
        company_id=company_id,
        domain_1=domain_for_pars,
        url=url_for_pars,
        chunks_inserted=inserted,
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
