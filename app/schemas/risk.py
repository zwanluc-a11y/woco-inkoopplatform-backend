from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict
from app.schemas.category import InkoopCategoryResponse


class RiskAssessmentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    category_id: int
    assessment_year: int
    yearly_spend: float
    supplier_count: int
    duration_years: float
    estimated_contract_value: float
    internal_threshold: float
    threshold_type: str
    risk_level: str
    policy_compliant: bool
    has_contract: bool
    notes: Optional[str] = None
    assessed_at: datetime
    assessed_by: Optional[int] = None
    category: Optional[InkoopCategoryResponse] = None


class RiskCalculateRequest(BaseModel):
    organization_id: int
    assessment_year: int
    category_id: Optional[int] = None
    duration_years: float = 4.0
