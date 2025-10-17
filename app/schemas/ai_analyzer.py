"""Pydantic-схемы для AI-анализатора компаний."""

from __future__ import annotations

from typing import Annotated, List, Optional

from pydantic import BaseModel, ConfigDict, Field, StringConstraints


# ИНН: 10 или 12 цифр
InnStr = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^\d{10}(\d{2})?$"),
]


class AiProduct(BaseModel):
    """Описание продукции, найденной анализатором."""

    name: str = Field(..., description="Название продукции или услуги")
    goods_group: Optional[str] = Field(
        None,
        description="Группа продукции ЕАЭС (GOODS_TYPE)",
    )
    domain: Optional[str] = Field(None, description="Домен источника")
    url: Optional[str] = Field(None, description="Ссылка на страницу с описанием")


class AiEquipment(BaseModel):
    """Описание оборудования, подобранного анализатором."""

    name: str = Field(..., description="Название оборудования")
    equip_group: Optional[str] = Field(
        None,
        description="Класс или группа оборудования (EQUIPMENT)",
    )
    domain: Optional[str] = Field(None, description="Домен источника")
    url: Optional[str] = Field(None, description="Ссылка на страницу с описанием")


class AiBlock(BaseModel):
    """AI-сводка по компании."""

    industry: Optional[str] = Field(None, description="Отрасль (человекочитаемо)")
    sites: List[str] = Field(default_factory=list, description="Список сайтов компании")
    products: List[AiProduct] = Field(
        default_factory=list,
        description="Список продукции, найденной анализатором",
    )
    equipment: List[AiEquipment] = Field(
        default_factory=list,
        description="Список оборудования, найденного анализатором",
    )
    utp: Optional[str] = Field(None, description="Уникальное торговое предложение")
    letter: Optional[str] = Field(None, description="Черновик письма для клиента")


class CompanyBlock(BaseModel):
    """Основные сведения о компании."""

    domain1: Optional[str] = Field(None, description="Основной домен")
    domain2: Optional[str] = Field(None, description="Дополнительный домен")


class AiAnalyzerResponse(BaseModel):
    """Канонический ответ AI-анализатора."""

    model_config = ConfigDict(extra="ignore")

    ok: bool = Field(True, description="Признак успешности ответа")
    inn: InnStr
    company: CompanyBlock = Field(
        default_factory=lambda: CompanyBlock(domain1=None, domain2=None),
        description="Информация о компании",
    )
    ai: AiBlock = Field(
        default_factory=lambda: AiBlock(
            industry=None,
            sites=[],
            products=[],
            equipment=[],
            utp=None,
            letter=None,
        ),
        description="AI-анализ компании",
    )
    note: Optional[str] = Field(
        None,
        description="Дополнительная информация для отладки",
    )
