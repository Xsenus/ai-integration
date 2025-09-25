# app/api/ai_analyzer.py
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, status

from app.schemas.ai_analyzer import (
    AiAnalyzerRequest,
    AiAnalyzerResponse,
    CompanyBlock,
    AiBlock,
    AiProduct,
    AiEquipment,
)

# Если решишь вынести бизнес-логику — импортируй из app.services.ai_analyzer
# from app.services.ai_analyzer import analyze_company_by_inn

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


@router.post(
    "/ai-analyzer",
    response_model=AiAnalyzerResponse,
    summary="AI-анализ компании по ИНН (POST)",
)
async def post_ai_analyzer(payload: AiAnalyzerRequest) -> AiAnalyzerResponse:
    """
    Принимает JSON вида:
    `{ "inn": "2455024746" }`

    Возвращает канонический объект, пригодный для мгновенного рендера на фронте.
    """
    inn = payload.inn

    # TODO: подключить реальную бизнес-логику:
    # result = await analyze_company_by_inn(inn)
    # return _canonical_response(**result)

    # Временный «пустой, но валидный» ответ (чтобы фронт сразу мог мапить данные):
    return _canonical_response(
        inn,
        note="stub: подключите app.services.ai_analyzer.analyze_company_by_inn",
    )


@router.get(
    "/{inn}/ai-analyzer",
    response_model=AiAnalyzerResponse,
    summary="AI-анализ компании по ИНН (GET)",
)
async def get_ai_analyzer(inn: str) -> AiAnalyzerResponse:
    """
    Идентичный по ответу эндпоинт GET /v1/lookup/{inn}/ai-analyzer.

    > Примечание: ИНН валидируется схематично — 10/12 цифр.
    """
    if not inn.isdigit() or len(inn) not in (10, 12):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="ИНН должен содержать 10 или 12 цифр",
        )

    # TODO: подключить реальную бизнес-логику:
    # result = await analyze_company_by_inn(inn)
    # return _canonical_response(**result)

    # Возвращаем заполненный пример, чтобы фронт увидел полный маппинг:
    example_products = [
        AiProduct(
            name="чёрный хлеб",
            goods_group="Группа 19",
            domain="example.com",
            url="https://example.com/bread",
        ),
        AiProduct(
            name="вафельные торты",
            goods_group="Группа 19",
            domain="example.com",
            url="https://example.com/waffle",
        ),
    ]
    example_equipment = [
        AiEquipment(
            name="Тестомесы/смесители",
            equip_group="Оборудование пищевое",
            domain="example.com",
            url="https://example.com/mixers",
        ),
        AiEquipment(
            name="Упаковочные машины",
            equip_group="Оборудование упаковочное",
            domain="example.com",
            url="https://example.com/pack",
        ),
    ]

    return _canonical_response(
        inn=inn,
        domain1="https://example.com",
        domain2="https://shop.example.com",
        industry="Пищевая промышленность / хлебопечение",
        sites=["https://example.com", "https://shop.example.com"],
        products=example_products,
        equipment=example_equipment,
        utp="🍞 От печи до полки: свежесть, контроль качества и быстрая логистика.",
        letter="Уважаемые коллеги,\nИзучили ваш сайт. Предлагаем...\n— Пункт 1\n— Пункт 2\nС уважением, ИРБИС",
        note="example payload",
    )
