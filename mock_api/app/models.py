from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class Promotion(BaseModel):
    promotion_id: str
    product_id: str
    promotion_name: str
    discount_type: str
    discount_value: float = Field(ge=0)
    starts_at: datetime
    ends_at: datetime
    status: str
    version: int = Field(ge=1)
    updated_at: datetime


class Pagination(BaseModel):
    page: int
    page_size: int
    total_pages: int
    total_records: int
    has_next: bool


class PromotionPage(BaseModel):
    data: list[Promotion]
    pagination: Pagination
