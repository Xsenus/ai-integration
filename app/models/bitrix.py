from __future__ import annotations

from typing import Optional

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
    management_surname_n_p: Mapped[Optional[str]] = mapped_column(String(256))  # 'Фамилия И.О.'
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
    okveds: Mapped[Optional[list]] = mapped_column(JSONB, doc="до 7 шт (массив объектов/пар)")

    year: Mapped[Optional[int]] = mapped_column(Integer)
    income: Mapped[Optional[float]] = mapped_column(Numeric(18, 2), doc="доходы по бух. отчетности")
    revenue: Mapped[Optional[float]] = mapped_column(Numeric(18, 2), doc="выручка по бух. отчетности")

    smb_type: Mapped[Optional[str]] = mapped_column(String(64), doc="тип документа")
    smb_category: Mapped[Optional[str]] = mapped_column(String(128), doc="Категория предприятия")
    smb_issue_date: Mapped[Optional[str]] = mapped_column(String(32), doc="Дата регистрации в реестре (как строка)")

    phones: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String), default=list)
    emails: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String), default=list)

    updated_at: Mapped["DateTime"] = mapped_column(
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
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    received_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_dadata_result_full_json_payload_gin", "payload", postgresql_using="gin"),
    )
