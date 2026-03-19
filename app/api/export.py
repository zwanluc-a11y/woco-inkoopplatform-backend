from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.api.deps import get_current_user_or_token, get_db
from app.models.user import User
from app.models.user_organization import UserOrganization
from app.services.export_service import ExportService

router = APIRouter(prefix="/organizations/{org_id}/export", tags=["export"])


def _check_membership(db: Session, user: User, org_id: int) -> None:
    """Inline membership check for export endpoints (which use token auth)."""
    # Platform users have implicit access to all orgs
    if user.platform_role in ("eigenaar", "beheerder"):
        return

    exists = (
        db.query(UserOrganization.id)
        .filter(
            UserOrganization.user_id == user.id,
            UserOrganization.organization_id == org_id,
        )
        .first()
    )
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Geen toegang tot deze organisatie",
        )


@router.get("/spend")
async def export_spend(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user_or_token)],
):
    """Export spend analysis to Excel."""
    _check_membership(db, current_user, org_id)
    service = ExportService(db)
    output = service.export_spend_analysis(org_id)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=spendanalyse.xlsx"},
    )


@router.get("/risk")
async def export_risk(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user_or_token)],
    year: int = Query(2025, description="Assessment year"),
):
    """Export risk assessment to Excel."""
    _check_membership(db, current_user, org_id)
    service = ExportService(db)
    output = service.export_risk_assessment(org_id, year)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=risicoanalyse_{year}.xlsx"},
    )


@router.get("/calendar")
async def export_calendar(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user_or_token)],
):
    """Export procurement calendar to Excel."""
    _check_membership(db, current_user, org_id)
    service = ExportService(db)
    output = service.export_calendar(org_id)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=inkoopkalender.xlsx"},
    )


@router.get("/report-pdf")
async def export_report_pdf(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user_or_token)],
    year: int = Query(2025, description="Assessment year"),
):
    """Generate comprehensive PDF report."""
    _check_membership(db, current_user, org_id)
    from app.services.pdf_service import PDFReportService

    service = PDFReportService(db)
    output = service.generate_report(org_id, year)

    return StreamingResponse(
        output,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=inkooprapportage_{year}.pdf"
        },
    )
