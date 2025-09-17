from __future__ import annotations

from typing import Optional, List
from pydantic import BaseModel, Field


class IngestRequest(BaseModel):
    """Тело запроса — любой JSON от DaData (один объект suggestion/data)."""
    payload: dict = Field(..., description="Полный JSON-ответ DaData по компании")
    # НЕОБЯЗАТЕЛЬНО: если знаем домен — можем передать
    domain: Optional[str] = Field(None, description="Адрес сайта (домен) компании")


class CompanySummaryOut(BaseModel):
    inn: Optional[str]
    short_name: Optional[str]
    short_name_opf: Optional[str]
    management_full_name: Optional[str]
    management_surname_n_p: Optional[str]
    management_surname: Optional[str]
    management_name: Optional[str]
    management_patronymic: Optional[str]
    management_post: Optional[str]
    branch_count: Optional[int]
    address: Optional[str]
    geo_lat: Optional[float]
    geo_lon: Optional[float]
    status: Optional[str]
    employee_count: Optional[int]
    main_okved: Optional[str]
    okveds: Optional[list]
    year: Optional[int]
    income: Optional[float]
    revenue: Optional[float]
    smb_type: Optional[str]
    smb_category: Optional[str]
    smb_issue_date: Optional[str]
    phones: Optional[List[str]]
    emails: Optional[List[str]]

    class Config:
        from_attributes = True


class OrgResponse(BaseModel):
    """Базовый ответ: сводка + последний raw JSON."""
    summary: Optional[CompanySummaryOut]
    raw_last: Optional[dict]


# -------- Карточка для расширенного ответа --------

class CompanyCard(BaseModel):
    # Поля карточки (по ТЗ)
    address: Optional[str] = None
    geo_lat: Optional[float] = None
    geo_lon: Optional[float] = None
    status: Optional[str] = None
    employee_count: Optional[int] = None
    main_okved: Optional[str] = None
    income: Optional[float] = None
    revenue: Optional[float] = None
    phones: Optional[List[str]] = None
    emails: Optional[List[str]] = None

    # Сгенерированные поля
    company_title: Optional[str] = None
    production_address_2024: Optional[str] = None


class OrgExtendedResponse(OrgResponse):
    """Расширенный ответ: всё из OrgResponse + карточка."""
    card: Optional[CompanyCard] = None
