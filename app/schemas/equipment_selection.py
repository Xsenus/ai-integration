"""Pydantic модели для ответа расчёта оборудования."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class ClientRow(BaseModel):
    id: int
    company_id: Optional[int] = None
    company_name: Optional[str] = None
    inn: Optional[str] = None
    domain_1: Optional[str] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None


class GoodsTypeRow(BaseModel):
    id: int
    goods_type: Optional[str] = None
    goods_type_id: Optional[int] = None
    goods_types_score: Optional[float] = None
    text_par_id: Optional[int] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None


class SiteEquipmentRow(BaseModel):
    id: int
    equipment: Optional[str] = None
    equipment_id: Optional[int] = None
    equipment_score: Optional[float] = None
    text_pars_id: Optional[int] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None


class ProdclassSourceRow(BaseModel):
    ai_row_id: int
    prodclass_id: int
    prodclass_name: Optional[str] = None
    prodclass_score: Optional[float] = None
    text_pars_id: Optional[int] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None


class WorkshopRow(BaseModel):
    id: int
    workshop_name: Optional[str] = None
    workshop_score: Optional[float] = None
    prodclass_id: Optional[int] = None
    company_id: Optional[int] = None


class EquipmentDetailRow(BaseModel):
    id: int
    equipment_name: Optional[str] = None
    workshop_id: Optional[int] = None
    equipment_score: Optional[float] = None
    equipment_score_real: Optional[float] = None
    equipment_score_max: Optional[float] = None
    score_1: Optional[float] = None
    factor: Optional[float] = None
    score_e1: Optional[float] = None
    path: str


class ProdclassDetail(BaseModel):
    prodclass_id: int
    prodclass_name: Optional[str] = None
    score_1: Optional[float] = None
    votes: int = 0
    path: str
    workshops: List[WorkshopRow] = Field(default_factory=list)
    fallback_industry_id: Optional[int] = None
    fallback_prodclass_ids: Optional[List[int]] = None
    fallback_workshops: Optional[List[WorkshopRow]] = None
    equipment: List[EquipmentDetailRow] = Field(default_factory=list)


class GoodsTypeScoreRow(BaseModel):
    goods_type_id: int
    crore_2: float


class EquipmentGoodsLinkRow(BaseModel):
    equipment_id: int
    goods_type_id: int
    crore_2: float
    crore_3: float
    score_e2: float
    equipment_name: Optional[str] = None


class EquipmentWayRow(BaseModel):
    id: int
    equipment_name: Optional[str] = None
    score: float


class EquipmentAllRow(EquipmentWayRow):
    source: str


class Equipment3WayDetailRow(BaseModel):
    equipment_id: int
    equipment_score: Optional[float] = None
    score_e3: Optional[float] = None


class SampleTable(BaseModel):
    title: str
    lines: List[str]


class EquipmentSelectionResponse(BaseModel):
    client: Optional[ClientRow] = None
    goods_types: List[GoodsTypeRow] = Field(default_factory=list)
    site_equipment: List[SiteEquipmentRow] = Field(default_factory=list)
    prodclass_rows: List[ProdclassSourceRow] = Field(default_factory=list)
    prodclass_details: List[ProdclassDetail] = Field(default_factory=list)
    goods_type_scores: List[GoodsTypeScoreRow] = Field(default_factory=list)
    goods_links: List[EquipmentGoodsLinkRow] = Field(default_factory=list)
    equipment_1way: List[EquipmentWayRow] = Field(default_factory=list)
    equipment_1way_details: List[EquipmentDetailRow] = Field(default_factory=list)
    equipment_2way: List[EquipmentWayRow] = Field(default_factory=list)
    equipment_2way_details: List[EquipmentGoodsLinkRow] = Field(default_factory=list)
    equipment_3way: List[EquipmentWayRow] = Field(default_factory=list)
    equipment_3way_details: List[Equipment3WayDetailRow] = Field(default_factory=list)
    equipment_all: List[EquipmentAllRow] = Field(default_factory=list)
    sample_tables: List[SampleTable] = Field(default_factory=list)
    log: List[str] = Field(default_factory=list)


__all__ = [
    "ClientRow",
    "GoodsTypeRow",
    "SiteEquipmentRow",
    "ProdclassSourceRow",
    "WorkshopRow",
    "EquipmentDetailRow",
    "ProdclassDetail",
    "GoodsTypeScoreRow",
    "EquipmentGoodsLinkRow",
    "EquipmentWayRow",
    "EquipmentAllRow",
    "Equipment3WayDetailRow",
    "SampleTable",
    "EquipmentSelectionResponse",
]
