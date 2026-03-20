from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.calendar_service import CalendarService

router = APIRouter(
    prefix="/organizations/{org_id}/calendar",
    tags=["calendar"],
)


class GenerateCalendarRequest(BaseModel):
    assessment_year: int = 2025


class UpdateCalendarItemRequest(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    target_start_date: Optional[str] = None
    target_publish_date: Optional[str] = None
    status: Optional[str] = None


class UpdatePhaseRequest(BaseModel):
    status: Optional[str] = None
    planned_start_date: Optional[str] = None
    planned_end_date: Optional[str] = None
    notes: Optional[str] = None


@router.post("/generate")
async def generate_calendar(
    org_id: int,
    data: GenerateCalendarRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Generate calendar items from risk assessments and expiring contracts."""
    service = CalendarService(db)
    try:
        items = service.generate_calendar(org_id, data.assessment_year)
        return {"items": items, "count": len(items)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("")
async def get_calendar(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get all calendar items."""
    service = CalendarService(db)
    items = service.get_calendar(org_id)
    return {"items": items, "count": len(items)}


@router.put("/items/{item_id}")
async def update_calendar_item(
    org_id: int,
    item_id: int,
    data: UpdateCalendarItemRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Update a calendar item."""
    service = CalendarService(db)
    updates = data.model_dump(exclude_unset=True)
    try:
        item = service.update_item(org_id, item_id, updates)
        return item
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/items/{item_id}/phases/{phase_id}")
async def update_phase(
    org_id: int,
    item_id: int,
    phase_id: int,
    data: UpdatePhaseRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Update a procurement phase."""
    service = CalendarService(db)
    updates = data.model_dump(exclude_unset=True)
    try:
        phase = service.update_phase(org_id, item_id, phase_id, updates)
        return phase
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
