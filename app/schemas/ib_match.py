from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class IbMatchRequest(BaseModel):
    """Запрос на присвоение соответствий из справочников IB."""

    client_id: int = Field(..., ge=1, description="Идентификатор из clients_requests.id")
    reembed_if_exists: bool = Field(
        False,
        description=(
            "Если true, заново генерирует эмбеддинги даже при наличии text_vector"
        ),
    )


class IbMatchInnRequest(BaseModel):
    """Запрос на присвоение соответствий по ИНН."""

    inn: str = Field(..., min_length=4, max_length=20, description="ИНН клиента")
    reembed_if_exists: bool = Field(
        False,
        description=(
            "Если true, заново генерирует эмбеддинги даже при наличии text_vector"
        ),
    )


class GoodsMatchItem(BaseModel):
    ai_goods_id: int = Field(..., description="ID строки в ai_site_goods_types")
    ai_goods_type: str = Field(..., description="Исходный текст типа продукции")
    match_ib_id: Optional[int] = Field(
        None, description="ID подобранного справочника ib_goods_types"
    )
    match_ib_name: Optional[str] = Field(
        None, description="Название подобранного справочника"
    )
    score: Optional[float] = Field(None, description="Косинусное сходство [0..1]")
    note: Optional[str] = Field(None, description="Комментарий по обработке строки")


class EquipmentMatchItem(BaseModel):
    ai_equip_id: int = Field(..., description="ID строки в ai_site_equipment")
    ai_equipment: str = Field(..., description="Исходный текст оборудования")
    match_ib_id: Optional[int] = Field(
        None, description="ID подобранного справочника ib_equipment"
    )
    match_ib_name: Optional[str] = Field(
        None, description="Название подобранного справочника"
    )
    score: Optional[float] = Field(None, description="Косинусное сходство [0..1]")
    note: Optional[str] = Field(None, description="Комментарий по обработке строки")


class IbMatchSummary(BaseModel):
    goods_processed: int = Field(..., description="Сколько строк товаров обработано")
    goods_updated: int = Field(..., description="Сколько строк товаров обновлено")
    goods_embeddings_generated: int = Field(
        ..., description="Сколько эмбеддингов товаров сгенерировано"
    )
    equipment_processed: int = Field(..., description="Сколько строк оборудования обработано")
    equipment_updated: int = Field(..., description="Сколько строк оборудования обновлено")
    equipment_embeddings_generated: int = Field(
        ..., description="Сколько эмбеддингов оборудования сгенерировано"
    )
    ib_goods_with_vectors: int = Field(
        ..., description="Количество позиций в ib_goods_types с валидными векторами"
    )
    ib_equipment_with_vectors: int = Field(
        ..., description="Количество позиций в ib_equipment с валидными векторами"
    )


class IbMatchResponse(BaseModel):
    client_id: int = Field(..., description="Обрабатываемый clients_requests.id")
    goods: List[GoodsMatchItem] = Field(
        default_factory=list, description="Итоговые соответствия для товаров"
    )
    equipment: List[EquipmentMatchItem] = Field(
        default_factory=list, description="Итоговые соответствия для оборудования"
    )
    summary: IbMatchSummary = Field(..., description="Итоговая статистика обработки")
    report: str = Field(..., description="Текстовый отчёт по шагам обработки")


__all__ = [
    "IbMatchRequest",
    "IbMatchInnRequest",
    "IbMatchResponse",
    "IbMatchSummary",
    "GoodsMatchItem",
    "EquipmentMatchItem",
]
