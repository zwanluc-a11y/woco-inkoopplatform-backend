from __future__ import annotations
from datetime import date, datetime
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    supplier_id: int
    import_session_id: int
    year: int
    period: Optional[int] = None
    booking_date: Optional[date] = None
    amount: float
    description: Optional[str] = None
    account_code: Optional[str] = None
    cost_center: Optional[str] = None
    raw_data: Optional[dict[str, Any]] = None
    created_at: datetime
