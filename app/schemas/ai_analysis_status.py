from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

AnalysisStatus = Literal["queued", "running", "completed", "failed", "stopped"]


class AiAnalysisCompanyStatus(BaseModel):
    inn: str = Field(..., description="ИНН компании")
    company_name: Optional[str] = Field(None, description="Название компании")
    analysis_status: AnalysisStatus = Field(..., description="Текущий статус AI-анализа")
    queued_at: Optional[datetime] = Field(None, description="Момент постановки задачи в очередь")
    analysis_started_at: Optional[datetime] = Field(None, description="Момент старта анализа")
    analysis_finished_at: Optional[datetime] = Field(None, description="Момент завершения анализа")
    analysis_duration_ms: int = Field(..., ge=0, description="Накопленная/финальная длительность в мс")
    analysis_progress: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Прогресс анализа в диапазоне [0..1]",
    )
    run_id: Optional[int] = Field(None, description="Идентификатор запуска (clients_requests.id)")


class AiAnalysisCompaniesResponse(BaseModel):
    items: list[AiAnalysisCompanyStatus]
    generated_at: datetime
