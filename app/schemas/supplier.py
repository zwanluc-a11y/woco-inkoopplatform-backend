from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class SupplierYearlySpendResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    supplier_id: int
    year: int
    total_amount: float
    transaction_count: int


class SupplierCategorizationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    supplier_id: int
    category_id: int
    source: str
    confidence: Optional[float] = None
    ai_reasoning: Optional[str] = None
    categorized_by: Optional[int] = None
    created_at: datetime
    updated_at: datetime


class SupplierResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    name: str
    supplier_code: Optional[str] = None
    normalized_name: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class SupplierDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    name: str
    supplier_code: Optional[str] = None
    normalized_name: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    yearly_spends: list[SupplierYearlySpendResponse] = []
    categorization: Optional[SupplierCategorizationResponse] = None
