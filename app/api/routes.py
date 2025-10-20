from __future__ import annotations

import logging

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.bitrix import get_bitrix_session
from app.db.postgres import get_postgres_engine
from app.schemas.equipment_selection import EquipmentSelectionResponse
from app.schemas.ib_match import IbMatchInnRequest, IbMatchRequest, IbMatchResponse
from app.services.analyze_health import probe_analyze_service
from app.services.equipment_selection import (
    EquipmentSelectionNotFound,
    compute_equipment_selection,
    resolve_client_request_id,
)
from app.services.ib_match import (
    IbMatchServiceError,
    assign_ib_matches,
    assign_ib_matches_by_inn,
)
from app.services.parse_site import ParseSiteRequest, ParseSiteResponse, run_parse_site

log = logging.getLogger("api.routes")
router = APIRouter(prefix="/v1")
ib_match_router = APIRouter(prefix="/ib-match", tags=["IB Matching"])
parse_site_router = APIRouter(prefix="/parse-site", tags=["Parse Site"])
equipment_selection_router = APIRouter(
    prefix="/equipment-selection", tags=["Equipment Selection"]
)


@router.get(
    "/analyze/health",
    summary="Проверка доступности внешнего сервиса анализа",
)
async def analyze_health_probe() -> JSONResponse:
    result = await probe_analyze_service(label="GET /v1/analyze/health")
    status_code = (
        status.HTTP_200_OK if result.get("ok") else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    if not result.get("detail"):
        result["detail"] = None
    return JSONResponse(status_code=status_code, content=result)


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
) -> ParseSiteResponse:
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
        pattern=r"^\d+$",
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
) -> ParseSiteResponse:
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
) -> EquipmentSelectionResponse:
    log.info(
        "equipment-selection GET: starting computation for clients_requests.id=%s",
        client_request_id,
    )
    engine = get_postgres_engine()
    if engine is None:
        log.error("equipment-selection GET: postgres engine is not configured")
        raise HTTPException(status_code=503, detail="Postgres engine is not configured")
    async with engine.begin() as conn:
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
    inn: str = Path(..., min_length=4, max_length=20, pattern=r"^\d+$"),
) -> EquipmentSelectionResponse:
    log.info("equipment-selection GET/by-inn: starting for INN %s", inn)
    engine = get_postgres_engine()
    if engine is None:
        log.error("equipment-selection GET/by-inn: postgres engine is not configured")
        raise HTTPException(status_code=503, detail="Postgres engine is not configured")
    async with engine.begin() as conn:
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
