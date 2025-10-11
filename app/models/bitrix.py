# app/models/bitrix.py
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import (
    String,
    Integer,
    Numeric,
    DateTime,
    func,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, DOUBLE_PRECISION, BIGINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseBitrix(DeclarativeBase):
    pass


class DaDataResult(BaseBitrix):
    """
    Сводная таблица по компании (нормализованные поля).
    PK = inn.
    """
    __tablename__ = "dadata_result"

    inn: Mapped[str] = mapped_column(String(20), primary_key=True, doc="ИНН")

    short_name: Mapped[Optional[str]] = mapped_column(String(512))
    short_name_opf: Mapped[Optional[str]] = mapped_column(String(512))

    management_full_name: Mapped[Optional[str]] = mapped_column(String(512))
    management_surname_n_p: Mapped[Optional[str]] = mapped_column(String(256), doc="Фамилия И.О.")
    management_surname: Mapped[Optional[str]] = mapped_column(String(256))
    management_name: Mapped[Optional[str]] = mapped_column(String(256))
    management_patronymic: Mapped[Optional[str]] = mapped_column(String(256))
    management_post: Mapped[Optional[str]] = mapped_column(String(256))

    branch_count: Mapped[Optional[int]] = mapped_column(Integer)

    address: Mapped[Optional[str]] = mapped_column(String(1024))
    geo_lat: Mapped[Optional[float]] = mapped_column(DOUBLE_PRECISION)
    geo_lon: Mapped[Optional[float]] = mapped_column(DOUBLE_PRECISION)

    status: Mapped[Optional[str]] = mapped_column(String(64))

    employee_count: Mapped[Optional[int]] = mapped_column(Integer, doc="ССЧ2024")

    main_okved: Mapped[Optional[str]] = mapped_column(String(32))
    # до 7 шт (массив строк или объектов {code,name,...})
    okveds: Mapped[Optional[list[dict[str, Any] | str]]] = mapped_column(JSONB)

    year: Mapped[Optional[int]] = mapped_column(Integer)
    income: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), doc="доходы по бух. отчетности")
    revenue: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), doc="выручка по бух. отчетности")

    smb_type: Mapped[Optional[str]] = mapped_column(String(64), doc="тип документа")
    smb_category: Mapped[Optional[str]] = mapped_column(String(128), doc="Категория предприятия")
    smb_issue_date: Mapped[Optional[str]] = mapped_column(String(32), doc="Дата регистрации в реестре (как строка)")

    phones: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String), default=list)
    emails: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String), default=list)
    web_sites: Mapped[Optional[str]] = mapped_column(String(2048))

    # Исторические показатели: маппинг на колонки с дефисами из БД
    revenue_1: Mapped[Optional[Decimal]] = mapped_column("revenue-1", Numeric(18, 0))
    revenue_2: Mapped[Optional[Decimal]] = mapped_column("revenue-2", Numeric(18, 0))
    revenue_3: Mapped[Optional[Decimal]] = mapped_column("revenue-3", Numeric(18, 0))

    income_1: Mapped[Optional[Decimal]] = mapped_column("income-1", Numeric(18, 0))
    income_2: Mapped[Optional[Decimal]] = mapped_column("income-2", Numeric(18, 0))
    income_3: Mapped[Optional[Decimal]] = mapped_column("income-3", Numeric(18, 0))

    employee_count_1: Mapped[Optional[int]] = mapped_column("employee_count-1", Integer)
    employee_count_2: Mapped[Optional[int]] = mapped_column("employee_count-2", Integer)
    employee_count_3: Mapped[Optional[int]] = mapped_column("employee_count-3", Integer)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        Index("ix_dadata_result_main_okved", "main_okved"),
    )


class DaDataResultFullJSON(BaseBitrix):
    """
    Полный JSON-ответ от DaData (источник истины).
    Для гибкого поиска по полям — GIN индекс по payload.
    """
    __tablename__ = "dadata_result_full_json"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    inn: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    source: Mapped[str] = mapped_column(String(32), default="dadata")
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_dadata_result_full_json_payload_gin", "payload", postgresql_using="gin"),
    )
