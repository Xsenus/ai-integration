from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class PipelineRequest(BaseModel):
    """Входные параметры для полного пайплайна."""

    inn: str | None = Field(
        None,
        min_length=4,
        max_length=20,
        description="ИНН клиента. Достаточно любого идентификатора, чтобы связать данные.",
    )
    site: str | None = Field(
        None,
        description="Сайт компании (домен или URL). Нормализуется до домена без www.",
    )
    pars_id: int | None = Field(
        None,
        description="Идентификатор записи pars_site.",
    )
    client_id: int | None = Field(
        None,
        description="Идентификатор клиента (clients_requests.id).",
    )
    run_analyze: bool = Field(
        False,
        description="Запускать ли шаг анализа перед сопоставлениями.",
    )
    analyze_options: Optional[dict[str, Any]] = Field(
        default=None,
        description="Настройки шага анализа (совместимы с /v1/analyze/{pars_id}).",
    )
    ib_match_options: Optional[dict[str, Any]] = Field(
        default=None,
        description="Опции для шага IB-match.",
    )

    @model_validator(mode="after")
    def _ensure_identifier_present(self) -> "PipelineRequest":  # noqa: D401
        """Проверяет, что указан хотя бы один идентификатор."""

        if not any([self.inn, self.site, self.pars_id, self.client_id]):
            raise ValueError(
                "Необходимо указать хотя бы один идентификатор: inn, site, pars_id или client_id."
            )
        return self


class ResolvedIdentifiers(BaseModel):
    """Итоговые значения идентификаторов после разрешения."""

    inn: str | None = None
    site: str | None = None
    pars_id: int | None = None
    client_id: int


class PipelineClient(BaseModel):
    """Сведения о выбранной записи клиента."""

    id: int
    company_name: str | None = None
    inn: str | None = None
    domain_1: str | None = None
    domain_2: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime | None = None


class PipelineParsSite(BaseModel):
    """Краткие сведения по записи pars_site."""

    id: int
    domain_1: str | None = None
    url: str | None = None
    created_at: datetime | None = None


class ParseSiteStage(BaseModel):
    """Блок с информацией о pars_site и клиенте."""

    client_id: int
    client: PipelineClient
    pars_sites: list[PipelineParsSite]
    selected_pars_id: int | None = None


class PipelineResponse(BaseModel):
    """Полный ответ пайплайна."""

    resolved: ResolvedIdentifiers
    parse_site: ParseSiteStage
    analyze: Optional[dict[str, Any]] = None
    ib_match: Optional[dict[str, Any]] = None
    equipment_selection: Optional[dict[str, Any]] = None
    duration_ms: int
