from __future__ import annotations
from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict
from app.schemas.supplier import SupplierResponse


class ContractCreate(BaseModel):
    name: str
    contract_number: Optional[str] = None
    contract_type: Optional[str] = None
    category_id: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    extension_options: Optional[str] = None
    max_end_date: Optional[date] = None
    estimated_value: Optional[float] = None
    is_ingekocht_via_procedure: bool = False
    status: str = "active"
    notes: Optional[str] = None
    supplier_ids: list[int] = []


class ContractUpdate(BaseModel):
    name: Optional[str] = None
    contract_number: Optional[str] = None
    contract_type: Optional[str] = None
    category_id: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    extension_options: Optional[str] = None
    max_end_date: Optional[date] = None
    estimated_value: Optional[float] = None
    is_ingekocht_via_procedure: Optional[bool] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    supplier_ids: Optional[list[int]] = None


class ContractResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    name: str
    contract_number: Optional[str] = None
    contract_type: Optional[str] = None
    category_id: Optional[int] = None
    category_name: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    extension_options: Optional[str] = None
    max_end_date: Optional[date] = None
    estimated_value: Optional[float] = None
    is_ingekocht_via_procedure: bool
    status: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    suppliers: list[SupplierResponse] = []
