from __future__ import annotations
from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field

VALID_ORG_TYPES = Literal[
    "woningcorporatie_klein", "woningcorporatie_middel",
    "woningcorporatie_groot", "overig",
]


class ThresholdResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    threshold_period: str
    diensten_leveringen: Optional[float] = 0
    werken: Optional[float] = 0
    ict_diensten: Optional[float] = 0
    advies_diensten: Optional[float] = 0
    is_default: Optional[bool] = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ThresholdUpdate(BaseModel):
    threshold_period: Optional[str] = None
    diensten_leveringen: Optional[float] = None
    werken: Optional[float] = None
    ict_diensten: Optional[float] = None
    advies_diensten: Optional[float] = None
    is_default: Optional[bool] = None


class OrganizationCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    org_type: VALID_ORG_TYPES
    description: Optional[str] = Field(None, max_length=2000)
    aantal_vhe: Optional[int] = Field(None, ge=1, description="Aantal verhuureenheden")


class OrganizationUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    org_type: Optional[VALID_ORG_TYPES] = None
    description: Optional[str] = Field(None, max_length=2000)
    aantal_vhe: Optional[int] = Field(None, ge=1, description="Aantal verhuureenheden")


class OrganizationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    org_type: Optional[str] = None
    description: Optional[str] = None
    aantal_vhe: Optional[int] = None
    created_by: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    thresholds: list[ThresholdResponse] = []
