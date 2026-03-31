"""Risk Assessment API endpoints."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.contract import Contract, ContractSupplier
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.user import User
from app.services.risk_service import RiskService

router = APIRouter(
    prefix="/organizations/{org_id}/risk",
    tags=["risk"],
)


class CalculateRiskRequest(BaseModel):
    assessment_year: int = 2025


@router.post("/calculate")
def calculate_risk(
    org_id: int,
    request: CalculateRiskRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Run risk assessment for all categorized spend."""
    service = RiskService(db)
    try:
        calculation = service.calculate_risk(
            org_id=org_id,
            assessment_year=request.assessment_year,
            user_id=user.id,
        )
        summary = service.get_risk_summary(org_id)
        return {
            "results": calculation["results"],
            "summary": summary,
            "assessment_year": request.assessment_year,
            "diagnostics": calculation["diagnostics"],
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/latest")
def get_latest_assessments(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get the most recent risk assessments."""
    service = RiskService(db)
    assessments = service.get_latest_assessments(org_id)

    assessment_year = assessments[0].assessment_year if assessments else 2025

    results = []
    for a in assessments:
        internal_threshold = float(a.internal_threshold)
        results.append(
            {
                "id": a.id,
                "category_id": a.category_id,
                "category_naam": a.category.inkooppakket if a.category else None,
                "category_nummer": a.category.nummer if a.category else None,
                "groep": a.category.groep if a.category else None,
                "soort_inkoop": a.threshold_type,
                "jaarlijkse_spend": float(a.yearly_spend),
                "leverancier_count": a.supplier_count,
                "verwachte_looptijd": a.duration_years,
                "geraamde_opdrachtwaarde": float(a.estimated_contract_value),
                "toepasselijke_drempel": internal_threshold,
                "percentage_van_drempel": (
                    round(float(a.estimated_contract_value) / internal_threshold * 100, 1)
                    if internal_threshold > 0
                    else 0
                ),
                "risk_level": a.risk_level,
                "has_contract": a.has_contract,
                "contract_end_date": None,
                "notes": a.notes,
            }
        )

    return {"results": results, "assessment_year": assessment_year}


@router.get("/summary")
def get_risk_summary(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get aggregated risk summary."""
    service = RiskService(db)
    return service.get_risk_summary(org_id)


@router.get("/years")
def get_available_years(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get years with spend data available for risk analysis."""
    service = RiskService(db)
    years = service.get_available_years(org_id)
    return {"years": years}


@router.get("/category/{category_id}/contracts")
def get_contracts_for_category(
    org_id: int,
    category_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get contracts linked to a specific PIANOo category (direct or via suppliers)."""
    from sqlalchemy import or_

    # Get supplier IDs in this category
    supplier_ids = [
        s.supplier_id
        for s in db.query(SupplierCategorization.supplier_id)
        .filter(
            SupplierCategorization.organization_id == org_id,
            SupplierCategorization.category_id == category_id,
        )
        .all()
    ]

    # Find contracts: direct category link OR via supplier overlap
    query = db.query(Contract).filter(Contract.organization_id == org_id)

    conditions = [Contract.category_id == category_id]
    if supplier_ids:
        # Subquery for contracts linked to these suppliers
        contract_ids_via_suppliers = (
            db.query(ContractSupplier.contract_id)
            .filter(ContractSupplier.supplier_id.in_(supplier_ids))
            .subquery()
        )
        conditions.append(Contract.id.in_(contract_ids_via_suppliers))

    contracts = query.filter(or_(*conditions)).all()

    results = []
    for c in contracts:
        results.append({
            "id": c.id,
            "name": c.name,
            "contract_number": c.contract_number,
            "contract_type": c.contract_type,
            "start_date": c.start_date.isoformat() if c.start_date else None,
            "end_date": c.end_date.isoformat() if c.end_date else None,
            "estimated_value": float(c.estimated_value) if c.estimated_value else None,
            "is_ingekocht_via_procedure": c.is_ingekocht_via_procedure,
            "status": c.status,
            "category_name": c.category.inkooppakket if c.category else None,
            "link_type": "direct" if c.category_id == category_id else "leveranciers",
        })

    return {"contracts": results}


@router.get("/category/{category_id}/suppliers")
def get_suppliers_for_category(
    org_id: int,
    category_id: int,
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get suppliers in a specific PIANOo category with their yearly spend."""
    query = (
        db.query(Supplier)
        .join(SupplierCategorization, SupplierCategorization.supplier_id == Supplier.id)
        .filter(
            SupplierCategorization.organization_id == org_id,
            SupplierCategorization.category_id == category_id,
        )
        .order_by(Supplier.name)
    )

    suppliers = query.all()
    results = []
    for s in suppliers:
        # Get spend for the requested year (or all years)
        spends = s.yearly_spends or []
        if year:
            spend_for_year = next(
                (ys.total_amount for ys in spends if ys.year == year), None
            )
            spend_amount = float(spend_for_year) if spend_for_year is not None else 0.0
        else:
            spend_amount = sum(float(ys.total_amount) for ys in spends)

        results.append({
            "id": s.id,
            "name": s.name,
            "supplier_code": s.supplier_code,
            "spend": spend_amount,
            "source": s.categorizations[0].source if s.categorizations else None,
            "confidence": s.categorizations[0].confidence if s.categorizations else None,
        })

    # Sort by spend descending
    results.sort(key=lambda r: -r["spend"])
    return {"suppliers": results}
