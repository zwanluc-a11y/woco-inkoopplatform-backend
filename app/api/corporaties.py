from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.models.woningcorporatie import WoningCorporatie

router = APIRouter(prefix="/corporaties", tags=["corporaties"])


@router.get("")
async def list_corporaties(
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None, min_length=1),
    provincie: Optional[str] = None,
    grootte: Optional[str] = None,
):
    """List all known woningcorporaties. No auth required (public reference data)."""
    query = db.query(WoningCorporatie)
    if search:
        safe = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = query.filter(WoningCorporatie.naam.ilike(f"%{safe}%", escape="\\"))
    if provincie:
        query = query.filter(WoningCorporatie.provincie == provincie)
    if grootte:
        query = query.filter(WoningCorporatie.grootte_klasse == grootte)
    query = query.order_by(WoningCorporatie.naam)
    corps = query.all()
    return [
        {
            "id": c.id,
            "l_nummer": c.l_nummer,
            "naam": c.naam,
            "provincie": c.provincie,
            "grootte_klasse": c.grootte_klasse,
            "aantal_vhe": c.aantal_vhe,
        }
        for c in corps
    ]
