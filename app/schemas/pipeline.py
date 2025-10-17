from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas.analyze_json import AnalyzeFromInnResponse
from app.schemas.equipment_selection import EquipmentSelectionResponse
from app.schemas.ib_match import IbMatchResponse
from app.schemas.org import OrgExtendedResponse
from app.services.parse_site import ParseSiteResponse


class PipelineFullRequest(BaseModel):
    """Входные параметры для полного пайплайна."""

    inn: str = Field(
        ...,
        min_length=4,
        max_length=20,
        pattern=r"^\d+$",
        description="ИНН клиента",
    )


class PipelineStepError(BaseModel):
    """Информация об ошибке шага пайплайна."""

    step: str = Field(..., description="Название шага пайплайна")
    status_code: int | None = Field(
        None,
        description="HTTP статус, если ошибка произошла на уровне HTTPException",
    )
    detail: str = Field(..., description="Описание ошибки")


class PipelineFullResponse(BaseModel):
    """Результат полного пайплайна."""

    inn: str
    lookup_card: OrgExtendedResponse | None = None
    parse_site: ParseSiteResponse | None = None
    analyze_json: AnalyzeFromInnResponse | None = None
    ib_match: IbMatchResponse | None = None
    equipment_selection: EquipmentSelectionResponse | None = None
    errors: list[PipelineStepError] = Field(default_factory=list)
    duration_ms: int


class PipelineAutoCandidate(BaseModel):
    """Сводка по статусу пайплайна для конкретного ИНН."""

    inn: str
    client_request_id: int | None = Field(
        None, description="ID записи clients_requests, если она существует"
    )
    has_clients_request: bool = Field(
        False, description="Пройдена ли стадия создания clients_requests"
    )
    has_pars_site: bool = Field(
        False, description="Есть ли pars_site для клиента"
    )
    has_ai_goods: bool = Field(
        False, description="Есть ли записи ai_site_goods_types"
    )
    has_ai_equipment: bool = Field(
        False, description="Есть ли записи ai_site_equipment"
    )
    has_equipment_selection: bool = Field(
        False, description="Есть ли результаты EQUIPMENT_ALL"
    )


class PipelineAutoResult(BaseModel):
    """Результат автоматического запуска пайплайна для ИНН."""

    status: PipelineAutoCandidate
    response: PipelineFullResponse | None = None
    error: str | None = Field(None, description="Описание ошибки, если запуск не удался")


class PipelineAutoResponse(BaseModel):
    """Ответ автоматического запуска пайплайна."""

    processed: list[PipelineAutoResult] = Field(default_factory=list)
    skipped: list[PipelineAutoCandidate] = Field(default_factory=list)
