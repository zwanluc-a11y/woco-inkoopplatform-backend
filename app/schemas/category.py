from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class InkoopCategoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    category_system: str = "aedes"
    groep: str
    sector: Optional[str] = None
    nummer: str
    inkooppakket: str
    definitie: Optional[str] = None
    soort_inkoop: Optional[str] = ""
    cpv_code: Optional[str] = None
    homogeen: Optional[bool] = None
    created_at: datetime


class CategorizationRequest(BaseModel):
    supplier_id: int
    category_id: int
    source: str = "manual"
    confidence: Optional[float] = None
    ai_reasoning: Optional[str] = None


class CategorizationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    supplier_id: int
    category_id: int
    percentage: float = 100.0
    source: str
    confidence: Optional[float] = None
    ai_reasoning: Optional[str] = None
    categorized_by: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    category: Optional[InkoopCategoryResponse] = None


class BulkCategorizationRequest(BaseModel):
    categorizations: list[CategorizationRequest]
