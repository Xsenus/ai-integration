# app/api/routes.py
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db.bitrix import get_bitrix_session
from app.models.bitrix import DaDataResult
from app.schemas.org import IngestRequest, OrgResponse, CompanySummaryOut
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


# ---------- Helpers ----------
async def _return_from_db_or_503(inn: str, session: AsyncSession, err_msg: str) -> OrgResponse:
    """
    Возвратить данные из БД, если они есть; иначе 503.
    Используется как fallback, когда DaData не отвечает.
    """
    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)

    if not summary and not raw_payload:
        # Нет сети и нет кэша — временная недоступность
        raise HTTPException(status_code=503, detail=f"{err_msg}; локальных данных по ИНН нет")

    return OrgResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
    )


# ---------- Ingest готового payload (без обращения к DaData) ----------
@router.post("/bitrix/ingest", response_model=OrgResponse)
async def ingest_dadata(
    body: IngestRequest,  # IngestRequest может содержать optional body.domain
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Принимает JSON с DaData (один объект suggestion/data), сохраняет:
    - raw в dadata_result_full_json (ровно одна строка на ИНН)
    - summary в dadata_result (upsert по inn)
    После успешного сохранения — best-effort запись во вторую БД (clients_requests),
    где domain_1 берётся из body.domain (если указан).
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

    # best-effort запись во вторую БД (domain_1 = body.domain)
    try:
        ok = await push_clients_request(summary_dict, domain=body.domain)
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


# ---------- Lookup через DaData (POST с телом) ----------
class LookupRequest(BaseModel):
    inn: str
    domain: Optional[str] = None  # НЕОБЯЗАТЕЛЬНО: домен сайта


@router.post("/lookup", response_model=OrgResponse)
async def lookup_post(
    dto: LookupRequest,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Тянем из DaData по ИНН, сохраняем raw+summary, возвращаем сводку и raw.
    Если DaData недоступна, возвращаем данные из БД (если есть), иначе 503.
    После успешного сохранения — best-effort запись во вторую БД (domain_1 = dto.domain).
    """
    inn = (dto.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup (POST) для ИНН %s: %s", inn, e)
        return await _return_from_db_or_503(inn, session, "DaData недоступна")

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

    # best-effort запись во вторую БД (domain_1 = dto.domain)
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


# ---------- Lookup через DaData (GET без тела) ----------
@router.get("/lookup/{inn}", response_model=OrgResponse)
async def lookup_get(
    inn: str,
    domain: Optional[str] = Query(None, description="Адрес сайта (домен), необязательно"),
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Аналог POST /v1/lookup, но ИНН в пути.
    Fallback: при недоступности DaData отдаём данные из БД (если есть), иначе 503.
    После успешного сохранения — best-effort запись во вторую БД (domain_1 = ?domain=...).
    """
    inn = (inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    try:
        suggestion = await find_party_by_inn(inn)
    except httpx.HTTPError as e:
        log.warning("DaData недоступна при lookup (GET) для ИНН %s: %s", inn, e)
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

    # best-effort запись во вторую БД (domain_1 = query param)
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


# ---------- Получить сохранённые данные без обращения к DaData ----------
@router.get("/org/{inn}", response_model=OrgResponse)
async def get_org(
    inn: str,
    session: AsyncSession = Depends(get_bitrix_session),
):
    """
    Быстрый доступ к сохранённым данным: сводка + последний raw по ИНН.
    """
    summary = await session.get(DaDataResult, inn)
    raw_payload = await get_last_raw(session, inn)

    if not summary and not raw_payload:
        raise HTTPException(status_code=404, detail="Данные по ИНН не найдены")

    return OrgResponse(
        summary=CompanySummaryOut.model_validate(summary) if summary else None,
        raw_last=raw_payload,
    )
