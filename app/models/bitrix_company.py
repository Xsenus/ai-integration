from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Index, String, Text, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseBitrix(DeclarativeBase):
    pass


class B24CompanyRaw(BaseBitrix):
    __tablename__ = "b24_companies_raw"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # ключ Bitrix24
    b24_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)

    # часто используемые поля — в отдельные колонки
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inn: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    kpp: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # последнее изменение карточки в B24 (строка/ts, Bitrix часто отдает строку)
    updated_at: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # полный ответ Bitrix24
    payload: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)

    # тех.поля
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False)

    __table_args__ = (
        UniqueConstraint("b24_id", name="uq_b24_companies_raw_b24_id"),
        Index("ix_b24_companies_raw_inn", "inn"),
        Index("ix_b24_companies_raw_title", "title"),
        Index("ix_b24_companies_raw_payload_gin", "payload", postgresql_using="gin"),
        Index("ix_b24_companies_raw_updated_at", "updated_at"),
    )

    @staticmethod
    def compute_sha256(obj: Dict[str, Any]) -> str:
        # deterministic hash (repr устраивает для технической дедупликации)
        raw = repr(obj).encode("utf-8", errors="ignore")
        return hashlib.sha256(raw).hexdigest()
