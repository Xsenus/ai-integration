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
        regex=r"^\d+$",
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
