# app/schemas/org.py
from __future__ import annotations

from typing import Optional, List, Any
from pydantic import BaseModel, Field, ConfigDict


class IngestRequest(BaseModel):
    """Тело запроса — любой JSON от DaData (один объект suggestion/data)."""
    payload: dict[str, Any] = Field(..., description="Полный JSON-ответ DaData по компании")
    # НЕОБЯЗАТЕЛЬНО: если знаем домен — можем передать
    domain: Optional[str] = Field(None, description="Адрес сайта (домен) компании")


class CompanySummaryOut(BaseModel):
    # Pydantic v2
    model_config = ConfigDict(from_attributes=True)

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
    okveds: Optional[List[Any]]
    year: Optional[int]
    income: Optional[float]
    revenue: Optional[float]
    smb_type: Optional[str]
    smb_category: Optional[str]
    smb_issue_date: Optional[str]
    phones: Optional[List[str]]
    emails: Optional[List[str]]


class OrgResponse(BaseModel):
    """Базовый ответ: сводка + последний raw JSON."""
    summary: Optional[CompanySummaryOut]
    raw_last: Optional[dict[str, Any]]


# -------- Расширенная карточка --------
class CompanyCard(BaseModel):
    # Поля из полного списка
    inn: Optional[str] = None
    short_name: Optional[str] = None
    short_name_opf: Optional[str] = None

    management_full_name: Optional[str] = None
    management_surname_n_p: Optional[str] = None
    management_surname: Optional[str] = None
    management_name: Optional[str] = None
    management_patronymic: Optional[str] = None
    management_post: Optional[str] = None

    branch_count: Optional[int] = None

    address: Optional[str] = None
    geo_lat: Optional[float] = None
    geo_lon: Optional[float] = None
    status: Optional[str] = None
    employee_count: Optional[int] = None

    # ОКВЭД
    main_okved: Optional[str] = None
    okved_main: Optional[str] = None
    okved_vtor_1: Optional[str] = None
    okved_vtor_2: Optional[str] = None
    okved_vtor_3: Optional[str] = None
    okved_vtor_4: Optional[str] = None
    okved_vtor_5: Optional[str] = None
    okved_vtor_6: Optional[str] = None
    okved_vtor_7: Optional[str] = None

    # Финансы / МСП / контакты
    year: Optional[int] = None
    income: Optional[float] = None
    revenue: Optional[float] = None
    smb_type: Optional[str] = None
    smb_category: Optional[str] = None
    smb_issue_date: Optional[str] = None
    phones: Optional[List[str]] = None
    emails: Optional[List[str]] = None

    # Сгенерированные поля
    company_title: Optional[str] = None
    production_address_2024: Optional[str] = None


class OrgExtendedResponse(OrgResponse):
    """Расширенный ответ: всё из OrgResponse + карточка."""
    card: Optional[CompanyCard] = None
