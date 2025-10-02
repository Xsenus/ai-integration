from __future__ import annotations

from typing import Annotated, List, Optional, Literal

from pydantic import BaseModel, Field, StringConstraints, ConfigDict

# ИНН: 10 или 12 цифр
InnStr = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^\d{10}(\d{2})?$"),
]


class AiAnalyzerRequest(BaseModel):
    """Тело запроса для POST /v1/lookup/ai-analyzer."""
    inn: InnStr = Field(..., description="ИНН (10 или 12 цифр)")


class AiProduct(BaseModel):
    name: str = Field(..., description="Название продукции/услуги")
    goods_group: Optional[str] = Field(None, description="Группа продукции ЕАЭС (GOODS_TYPE)")
    domain: Optional[str] = Field(None, description="Домен")
    url: Optional[str] = Field(None, description="Ссылка на страницу")


class AiEquipment(BaseModel):
    name: str = Field(..., description="Название оборудования")
    equip_group: Optional[str] = Field(None, description="Класс/группа EQUIPMENT")
    domain: Optional[str] = Field(None, description="Домен")
    url: Optional[str] = Field(None, description="Ссылка на страницу")


class AiBlock(BaseModel):
    industry: Optional[str] = Field(None, description="Отрасль (человекочитаемо)")
    sites: List[str] = Field(default_factory=list, description="Список сайтов (до 2 шт.)")
    products: List[AiProduct] = Field(default_factory=list, description="Номенклатура (AI)")
    equipment: List[AiEquipment] = Field(default_factory=list, description="Оборудование (AI)")
    utp: Optional[str] = Field(None, description="УТП (с эмодзи)")
    letter: Optional[str] = Field(None, description="Письмо (с переводами строк)")


class CompanyBlock(BaseModel):
    domain1: Optional[str] = Field(None, description="Основной домен/сайт")
    domain2: Optional[str] = Field(None, description="Второй домен/сайт")


class AiAnalyzerResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ok: bool = True
    inn: InnStr
    company: CompanyBlock = Field(
        default_factory=lambda: CompanyBlock(domain1=None, domain2=None)
    )
    ai: AiBlock = Field(
        default_factory=lambda: AiBlock(
            industry=None, sites=[], products=[], equipment=[], utp=None, letter=None
        )
    )
    note: Optional[str] = Field(None, description="Опциональная разработческая пометка")


class BulkAiAnalyzeLaunchResponse(BaseModel):
    """Результат запуска пакетного анализа компаний."""

    status: Literal["started", "already_running", "empty", "disabled"]
    queued: Optional[int] = Field(None, description="Сколько организаций поставлено в очередь")
    detail: Optional[str] = Field(None, description="Дополнительная информация о запуске")
