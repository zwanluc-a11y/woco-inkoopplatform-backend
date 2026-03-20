from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
from app.models.contract import Contract, ContractSupplier
from app.models.organization import Organization
from app.models.category import InkoopCategory
from app.models.risk_assessment import RiskAssessment
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.user import User
from app.models.user_organization import UserOrganization

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/stats")
async def dashboard_stats(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get dashboard statistics scoped to user's organizations."""
    # Platform users (eigenaar/beheerder) see all organizations
    if current_user.platform_role in ("eigenaar", "beheerder"):
        user_org_ids = [
            o.id for o in db.query(Organization.id).all()
        ]
    else:
        user_org_ids = [
            m.organization_id
            for m in db.query(UserOrganization.organization_id)
            .filter(UserOrganization.user_id == current_user.id)
            .all()
        ]

    org_count = len(user_org_ids)

    if not user_org_ids:
        return {
            "organizations": 0,
            "total_suppliers": 0,
            "categorized_suppliers": 0,
            "categorization_percentage": 0,
            "total_spend": 0.0,
            "expiring_contracts": 0,
            "risk_high_count": 0,
        }

    total_suppliers = (
        db.query(func.count(Supplier.id))
        .filter(Supplier.organization_id.in_(user_org_ids))
        .scalar() or 0
    )

    categorized = (
        db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
        .filter(SupplierCategorization.organization_id.in_(user_org_ids))
        .scalar() or 0
    )

    total_spend = (
        db.query(func.sum(SupplierYearlySpend.total_amount))
        .filter(SupplierYearlySpend.organization_id.in_(user_org_ids))
        .scalar() or 0
    )

    today = date.today()
    expiring_contracts = (
        db.query(func.count(Contract.id))
        .filter(
            Contract.organization_id.in_(user_org_ids),
            Contract.end_date != None,
            Contract.end_date <= date(today.year + 1, today.month, today.day),
            Contract.end_date >= today,
        )
        .scalar() or 0
    )

    risk_high = (
        db.query(func.count(RiskAssessment.id))
        .filter(
            RiskAssessment.organization_id.in_(user_org_ids),
            RiskAssessment.risk_level == "offertetraject",
        )
        .scalar() or 0
    )

    return {
        "organizations": org_count,
        "total_suppliers": total_suppliers,
        "categorized_suppliers": categorized,
        "categorization_percentage": round(
            (categorized / total_suppliers * 100) if total_suppliers > 0 else 0, 1
        ),
        "total_spend": float(total_spend),
        "expiring_contracts": expiring_contracts,
        "risk_high_count": risk_high,
    }


@router.get("/organizations/{org_id}/overview")
async def organization_overview(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get rich overview data for the organization dashboard."""
    today = date.today()
    current_year = today.year

    # --- Subquery: niet-beïnvloedbare supplier IDs ---
    niet_beinvloedbaar_sids = (
        db.query(Supplier.id)
        .filter(Supplier.organization_id == org_id, Supplier.is_beinvloedbaar == False)  # noqa: E712
        .subquery()
    )

    # --- Spend trend (all years) — only beïnvloedbaar ---
    yearly_spends = (
        db.query(
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount).label("total"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label("suppliers"),
        )
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .group_by(SupplierYearlySpend.year)
        .order_by(SupplierYearlySpend.year)
        .all()
    )
    spend_trend = [
        {"year": r.year, "total_spend": float(r.total), "supplier_count": r.suppliers}
        for r in yearly_spends
    ]

    # Also get ALL spend (incl niet-beïnvloedbaar) for reference
    yearly_spends_all = (
        db.query(
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount).label("total"),
        )
        .filter(SupplierYearlySpend.organization_id == org_id)
        .group_by(SupplierYearlySpend.year)
        .all()
    )
    spend_map_all = {r.year: float(r.total) for r in yearly_spends_all}
    most_recent_year = max(spend_map_all.keys()) if spend_map_all else current_year

    # Beïnvloedbaar spend map
    spend_map = {r.year: float(r.total) for r in yearly_spends}
    total_spend_current = spend_map.get(most_recent_year, 0)
    total_spend_previous = spend_map.get(most_recent_year - 1, 0)

    # --- Niet-beïnvloedbaar spend (current + previous year) ---
    nb_spend_current = spend_map_all.get(most_recent_year, 0) - total_spend_current
    nb_spend_previous = spend_map_all.get(most_recent_year - 1, 0) - total_spend_previous

    # --- Categorization progress (all suppliers) ---
    supplier_count = (
        db.query(func.count(Supplier.id))
        .filter(Supplier.organization_id == org_id)
        .scalar() or 0
    )
    categorized_count = (
        db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
        .filter(SupplierCategorization.organization_id == org_id)
        .scalar() or 0
    )

    # --- Spend by PIANOo groep — only beïnvloedbaar (percentage-weighted) ---
    groep_data = (
        db.query(
            InkoopCategory.groep,
            func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("total_spend"),
            func.count(InkoopCategory.id.distinct()).label("category_count"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
        )
        .join(SupplierCategorization, SupplierCategorization.category_id == InkoopCategory.id)
        .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
        .filter(
            SupplierCategorization.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .group_by(InkoopCategory.groep)
        .order_by(func.sum(SupplierYearlySpend.total_amount).desc())
        .all()
    )
    spend_by_groep = [
        {
            "groep": r.groep,
            "total_spend": float(r.total_spend),
            "category_count": r.category_count,
            "supplier_count": r.supplier_count,
        }
        for r in groep_data
    ]

    # --- Risk summary (all suppliers — risk is independent of beïnvloedbaar) ---
    risk_data = (
        db.query(
            RiskAssessment.risk_level,
            func.count(RiskAssessment.id).label("count"),
            func.sum(RiskAssessment.estimated_contract_value).label("total_value"),
        )
        .filter(RiskAssessment.organization_id == org_id)
        .group_by(RiskAssessment.risk_level)
        .all()
    )
    risk_summary = {
        "offertetraject": {"count": 0, "total_value": 0.0},
        "meervoudig_onderhands": {"count": 0, "total_value": 0.0},
        "enkelvoudig_onderhands": {"count": 0, "total_value": 0.0},
        "vrije_inkoop": {"count": 0, "total_value": 0.0},
    }
    for r in risk_data:
        if r.risk_level in risk_summary:
            risk_summary[r.risk_level] = {
                "count": r.count,
                "total_value": float(r.total_value or 0),
            }

    # Above threshold = aanbesteden + onderzoek
    above_threshold_count = risk_summary["offertetraject"]["count"] + risk_summary["meervoudig_onderhands"]["count"]
    above_threshold_value = risk_summary["offertetraject"]["total_value"] + risk_summary["meervoudig_onderhands"]["total_value"]

    # --- Expiring contracts (all — contract-level, independent of beïnvloedbaar) ---
    future_limit = date(today.year + 2, today.month, today.day) if today.month <= 6 else date(today.year + 1, 12, 31)
    expiring_contracts_query = (
        db.query(Contract)
        .filter(
            Contract.organization_id == org_id,
            Contract.end_date != None,
            Contract.end_date >= today,
            Contract.end_date <= future_limit,
        )
        .order_by(Contract.end_date.asc())
        .limit(10)
        .all()
    )
    expiring_contracts_list = []
    for c in expiring_contracts_query:
        days_remaining = (c.end_date - today).days if c.end_date else 0
        expiring_contracts_list.append({
            "id": c.id,
            "name": c.name,
            "end_date": c.end_date.isoformat() if c.end_date else None,
            "estimated_value": float(c.estimated_value) if c.estimated_value else None,
            "days_remaining": days_remaining,
            "category_name": c.category.inkooppakket if c.category else None,
            "status": c.status,
        })

    expiring_total_value = sum(
        float(c.estimated_value or 0)
        for c in expiring_contracts_query
    )

    # --- Top 10 suppliers by spend — only beïnvloedbaar ---
    top_suppliers_data = (
        db.query(
            Supplier.id,
            Supplier.name,
            SupplierYearlySpend.total_amount,
        )
        .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == Supplier.id)
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            Supplier.is_beinvloedbaar == True,  # noqa: E712
        )
        .order_by(SupplierYearlySpend.total_amount.desc())
        .limit(10)
        .all()
    )
    top_suppliers = [
        {"id": r.id, "name": r.name, "total_spend": float(r.total_amount)}
        for r in top_suppliers_data
    ]

    # --- Pareto / Spend concentration — only beïnvloedbaar ---
    all_supplier_spends = (
        db.query(SupplierYearlySpend.total_amount)
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .order_by(SupplierYearlySpend.total_amount.desc())
        .all()
    )
    total_supplier_count_year = len(all_supplier_spends)
    total_spend_year = sum(float(r.total_amount) for r in all_supplier_spends)
    pareto_suppliers_for_80 = 0
    pareto_cumulative = 0.0
    if total_spend_year > 0:
        threshold_80 = total_spend_year * 0.80
        for r in all_supplier_spends:
            pareto_cumulative += float(r.total_amount)
            pareto_suppliers_for_80 += 1
            if pareto_cumulative >= threshold_80:
                break
    pareto_percentage = round(
        (pareto_suppliers_for_80 / total_supplier_count_year * 100)
        if total_supplier_count_year > 0 else 0, 1
    )

    # --- Uncategorized spend — only beïnvloedbaar ---
    # Use distinct supplier_ids to avoid double-counting multi-category suppliers
    categorized_sids = (
        db.query(SupplierCategorization.supplier_id.distinct())
        .filter(SupplierCategorization.organization_id == org_id)
        .subquery()
    )
    categorized_spend = (
        db.query(func.sum(SupplierYearlySpend.total_amount))
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            SupplierYearlySpend.supplier_id.in_(categorized_sids),
            ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .scalar() or 0
    )
    uncategorized_spend = total_spend_year - float(categorized_spend)

    # --- Contract coverage / Maverick spend — only beïnvloedbaar ---
    suppliers_with_contract = (
        db.query(ContractSupplier.supplier_id.distinct())
        .join(Contract, Contract.id == ContractSupplier.contract_id)
        .filter(Contract.organization_id == org_id)
        .subquery()
    )
    contracted_spend = (
        db.query(func.sum(SupplierYearlySpend.total_amount))
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            SupplierYearlySpend.supplier_id.in_(suppliers_with_contract),
            ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .scalar() or 0
    )
    contracted_spend_float = float(contracted_spend)
    maverick_spend = total_spend_year - contracted_spend_float
    contract_coverage_pct = round(
        (contracted_spend_float / total_spend_year * 100)
        if total_spend_year > 0 else 0, 1
    )

    # --- Spend per procurement type — only beïnvloedbaar (percentage-weighted) ---
    type_data = (
        db.query(
            InkoopCategory.soort_inkoop,
            func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("total_spend"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
        )
        .join(SupplierCategorization, SupplierCategorization.category_id == InkoopCategory.id)
        .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
        .filter(
            SupplierCategorization.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .group_by(InkoopCategory.soort_inkoop)
        .order_by(func.sum(SupplierYearlySpend.total_amount).desc())
        .all()
    )
    spend_by_type = [
        {
            "type": r.soort_inkoop,
            "total_spend": float(r.total_spend),
            "supplier_count": r.supplier_count,
        }
        for r in type_data
    ]

    # --- Top growers / decliners per category — only beïnvloedbaar ---
    previous_year = most_recent_year - 1
    if previous_year in spend_map:
        cat_current = (
            db.query(
                SupplierCategorization.category_id,
                InkoopCategory.inkooppakket,
                InkoopCategory.groep,
                func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("spend"),
            )
            .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
            .join(InkoopCategory, InkoopCategory.id == SupplierCategorization.category_id)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(SupplierCategorization.category_id, InkoopCategory.inkooppakket, InkoopCategory.groep)
            .all()
        )
        cat_previous = (
            db.query(
                SupplierCategorization.category_id,
                func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("spend"),
            )
            .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
            .filter(
                SupplierCategorization.organization_id == org_id,
                SupplierYearlySpend.year == previous_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .group_by(SupplierCategorization.category_id)
            .all()
        )
        prev_map = {r.category_id: float(r.spend) for r in cat_previous}

        growth_list = []
        for r in cat_current:
            cur = float(r.spend)
            prev = prev_map.get(r.category_id, 0)
            if prev > 0:
                growth_pct = round((cur - prev) / prev * 100, 1)
            elif cur > 0:
                growth_pct = 100.0  # new category
            else:
                growth_pct = 0.0
            growth_list.append({
                "category_name": r.inkooppakket,
                "groep": r.groep,
                "current_spend": cur,
                "previous_spend": prev,
                "growth_pct": growth_pct,
                "absolute_change": round(cur - prev, 2),
            })

        growth_list.sort(key=lambda x: x["growth_pct"], reverse=True)
        top_growers = growth_list[:5]
        top_decliners = sorted(
            [g for g in growth_list if g["growth_pct"] < 0],
            key=lambda x: x["growth_pct"],
        )[:5]
    else:
        top_growers = []
        top_decliners = []

    # --- New & lost suppliers — only beïnvloedbaar ---
    if previous_year in spend_map:
        current_sids = set(
            r[0] for r in
            db.query(SupplierYearlySpend.supplier_id)
            .filter(
                SupplierYearlySpend.organization_id == org_id,
                SupplierYearlySpend.year == most_recent_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .all()
        )
        previous_sids = set(
            r[0] for r in
            db.query(SupplierYearlySpend.supplier_id)
            .filter(
                SupplierYearlySpend.organization_id == org_id,
                SupplierYearlySpend.year == previous_year,
                ~SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
            )
            .all()
        )
        new_supplier_ids = current_sids - previous_sids
        lost_supplier_ids = previous_sids - current_sids

        new_supplier_spend = 0.0
        if new_supplier_ids:
            new_supplier_spend = float(
                db.query(func.sum(SupplierYearlySpend.total_amount))
                .filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == most_recent_year,
                    SupplierYearlySpend.supplier_id.in_(new_supplier_ids),
                )
                .scalar() or 0
            )

        lost_supplier_spend = 0.0
        if lost_supplier_ids:
            lost_supplier_spend = float(
                db.query(func.sum(SupplierYearlySpend.total_amount))
                .filter(
                    SupplierYearlySpend.organization_id == org_id,
                    SupplierYearlySpend.year == previous_year,
                    SupplierYearlySpend.supplier_id.in_(lost_supplier_ids),
                )
                .scalar() or 0
            )

        supplier_dynamics = {
            "new_count": len(new_supplier_ids),
            "new_spend": new_supplier_spend,
            "lost_count": len(lost_supplier_ids),
            "lost_spend": lost_supplier_spend,
            "compare_year": previous_year,
        }
    else:
        supplier_dynamics = None

    # --- Niet-beïnvloedbaar sectie ---
    nb_suppliers_data = (
        db.query(
            Supplier.id,
            Supplier.name,
            SupplierYearlySpend.total_amount,
        )
        .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == Supplier.id)
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            Supplier.is_beinvloedbaar == False,  # noqa: E712
        )
        .order_by(SupplierYearlySpend.total_amount.desc())
        .limit(20)
        .all()
    )
    nb_supplier_count = (
        db.query(func.count(Supplier.id))
        .filter(
            Supplier.organization_id == org_id,
            Supplier.is_beinvloedbaar == False,  # noqa: E712
        )
        .scalar() or 0
    )
    # Niet-beïnvloedbaar spend by groep (percentage-weighted)
    nb_groep_data = (
        db.query(
            InkoopCategory.groep,
            func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("total_spend"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
        )
        .join(SupplierCategorization, SupplierCategorization.category_id == InkoopCategory.id)
        .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
        .filter(
            SupplierCategorization.organization_id == org_id,
            SupplierYearlySpend.year == most_recent_year,
            SupplierYearlySpend.supplier_id.in_(niet_beinvloedbaar_sids),
        )
        .group_by(InkoopCategory.groep)
        .order_by(func.sum(SupplierYearlySpend.total_amount).desc())
        .all()
    )

    niet_beinvloedbaar = {
        "total_spend": round(nb_spend_current, 2),
        "total_spend_previous": round(nb_spend_previous, 2),
        "supplier_count": nb_supplier_count,
        "suppliers": [
            {"id": r.id, "name": r.name, "total_spend": float(r.total_amount)}
            for r in nb_suppliers_data
        ],
        "spend_by_groep": [
            {
                "groep": r.groep,
                "total_spend": float(r.total_spend),
                "supplier_count": r.supplier_count,
            }
            for r in nb_groep_data
        ],
    }

    return {
        "total_spend_current_year": total_spend_current,
        "total_spend_previous_year": total_spend_previous,
        "total_spend_all": spend_map_all.get(most_recent_year, 0),
        "spend_year": most_recent_year,
        "categorization": {
            "total": supplier_count,
            "categorized": categorized_count,
            "percentage": round(
                (categorized_count / supplier_count * 100) if supplier_count > 0 else 0, 1
            ),
            "uncategorized_spend": round(uncategorized_spend, 2),
        },
        "above_threshold": {
            "count": above_threshold_count,
            "total_value": above_threshold_value,
        },
        "expiring_contracts": {
            "count": len(expiring_contracts_list),
            "total_value": expiring_total_value,
        },
        "spend_by_groep": spend_by_groep,
        "risk_summary": risk_summary,
        "expiring_contracts_list": expiring_contracts_list,
        "spend_trend": spend_trend,
        "top_suppliers": top_suppliers,
        "pareto": {
            "suppliers_for_80_pct": pareto_suppliers_for_80,
            "total_suppliers": total_supplier_count_year,
            "percentage": pareto_percentage,
        },
        "contract_coverage": {
            "covered_spend": contracted_spend_float,
            "maverick_spend": round(maverick_spend, 2),
            "coverage_pct": contract_coverage_pct,
        },
        "spend_by_type": spend_by_type,
        "category_growth": {
            "top_growers": top_growers,
            "top_decliners": top_decliners,
        },
        "supplier_dynamics": supplier_dynamics,
        "niet_beinvloedbaar": niet_beinvloedbaar,
    }


@router.post("/organizations/{org_id}/overview/recommendations")
@limiter.limit("3/minute")
async def generate_recommendations(
    org_id: int,
    request: Request,  # required by slowapi
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Generate AI-powered procurement recommendations based on overview data."""
    from app.services.recommendation_service import generate_recommendations as gen_recs

    org = db.query(Organization).get(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organisatie niet gevonden")

    # Get overview data (reuse the existing overview logic)
    overview_data = await organization_overview(org_id, db, current_user)

    try:
        recommendations = gen_recs(db, org, overview_data)
        return {
            "recommendations": recommendations,
            "generated_at": datetime.utcnow().isoformat(),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("AI recommendations failed for org %d", org_id)
        raise HTTPException(
            status_code=500,
            detail=f"Fout bij het genereren van aanbevelingen: {type(e).__name__}: {str(e)}",
        )


@router.get("/organizations/{org_id}/stats")
async def organization_stats(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get stats for a specific organization."""
    supplier_count = (
        db.query(func.count(Supplier.id))
        .filter(Supplier.organization_id == org_id)
        .scalar() or 0
    )

    categorized = (
        db.query(func.count(func.distinct(SupplierCategorization.supplier_id)))
        .filter(SupplierCategorization.organization_id == org_id)
        .scalar() or 0
    )

    yearly_spends = (
        db.query(
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount).label("total"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label("suppliers"),
        )
        .filter(SupplierYearlySpend.organization_id == org_id)
        .group_by(SupplierYearlySpend.year)
        .order_by(SupplierYearlySpend.year)
        .all()
    )

    today = date.today()
    contract_count = (
        db.query(func.count(Contract.id))
        .filter(Contract.organization_id == org_id)
        .scalar() or 0
    )

    risk_counts = (
        db.query(RiskAssessment.risk_level, func.count(RiskAssessment.id))
        .filter(RiskAssessment.organization_id == org_id)
        .group_by(RiskAssessment.risk_level)
        .all()
    )
    risks = {level: count for level, count in risk_counts}

    return {
        "supplier_count": supplier_count,
        "categorized_count": categorized,
        "categorization_percentage": round(
            (categorized / supplier_count * 100) if supplier_count > 0 else 0, 1
        ),
        "yearly_spends": [
            {
                "year": r.year,
                "total": float(r.total),
                "suppliers": r.suppliers,
            }
            for r in yearly_spends
        ],
        "contract_count": contract_count,
        "risk_aanbesteden": risks.get("offertetraject", 0),
        "risk_onderzoek": risks.get("meervoudig_onderhands", 0),
        "risk_monitoren": risks.get("enkelvoudig_onderhands", 0),
        "risk_akkoord": risks.get("vrije_inkoop", 0),
    }
