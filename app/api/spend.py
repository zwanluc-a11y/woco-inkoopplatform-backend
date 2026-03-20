from __future__ import annotations

import re
from collections import defaultdict
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.contract import Contract, ContractSupplier
from app.models.category import InkoopCategory
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.user import User

router = APIRouter(
    prefix="/organizations/{org_id}/spend",
    tags=["spend"],
)


@router.get("/summary")
async def spend_summary(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    import logging as _log
    _logger = _log.getLogger(__name__)
    try:
        _logger.info("spend_summary called for org_id=%s, user=%s", org_id, current_user.id)
        rows = (
            db.query(
                SupplierYearlySpend.year,
                func.sum(SupplierYearlySpend.total_amount).label("total_spend"),
                func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
                func.sum(SupplierYearlySpend.transaction_count).label("transaction_count"),
            )
            .filter(SupplierYearlySpend.organization_id == org_id)
            .group_by(SupplierYearlySpend.year)
            .order_by(SupplierYearlySpend.year)
            .all()
        )
        result = [
            {
                "year": r.year,
                "total_spend": float(r.total_spend or 0),
                "supplier_count": r.supplier_count,
                "transaction_count": r.transaction_count,
            }
            for r in rows
        ]
        _logger.info("spend_summary returning %d rows", len(result))
        return result
    except Exception as e:
        _logger.exception("spend_summary CRASHED: %s", e)
        raise


@router.get("/debug-test")
async def debug_test(org_id: int):
    """No-auth debug endpoint to test connectivity."""
    return {"ok": True, "org_id": org_id, "message": "debug endpoint works"}


@router.get("/debug-summary")
async def debug_summary(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Debug: same as summary but with explicit error handling."""
    import logging as _log
    _logger = _log.getLogger(__name__)
    _logger.info("debug_summary: start for org_id=%s user=%s", org_id, current_user.id)

    # Step 1: test auth only (return early)
    return [{"year": 2025, "total_spend": 0, "supplier_count": 0, "transaction_count": 0}]


@router.get("/pivot")
async def spend_pivot(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    years: Optional[str] = None,
    min_spend: float = 0,
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    # Get all suppliers with their yearly spends
    query = db.query(Supplier).filter(Supplier.organization_id == org_id)
    if search:
        safe_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = query.filter(Supplier.name.ilike(f"%{safe_search}%", escape="\\"))
    
    suppliers = query.all()

    # Build set of supplier IDs that have at least one contract
    contract_supplier_rows = (
        db.query(ContractSupplier.supplier_id, Contract.name)
        .join(Contract, Contract.id == ContractSupplier.contract_id)
        .filter(Contract.organization_id == org_id)
        .all()
    )
    supplier_contracts: dict[int, list[str]] = {}
    for row in contract_supplier_rows:
        supplier_contracts.setdefault(row.supplier_id, []).append(row.name)

    result = []
    year_list = [int(y) for y in years.split(",")] if years else None

    for s in suppliers:
        spends = {}
        total = 0
        for ys in s.yearly_spends:
            if year_list and ys.year not in year_list:
                continue
            spends[str(ys.year)] = float(ys.total_amount)
            total += float(ys.total_amount)
        
        if abs(total) < min_spend:
            continue
        
        cats = s.categorizations or []
        primary = max(cats, key=lambda c: c.percentage, default=None) if cats else None
        categories = []
        for c in cats:
            if c.category:
                categories.append({
                    "category_id": c.category_id,
                    "category_name": c.category.inkooppakket,
                    "percentage": c.percentage,
                })

        contracts_for_supplier = supplier_contracts.get(s.id, [])
        result.append({
            "id": s.id,
            "name": s.name,
            "supplier_code": s.supplier_code,
            "category_id": primary.category_id if primary else None,
            "category_name": primary.category.inkooppakket if primary and primary.category else None,
            "categories": categories,
            "is_beinvloedbaar": s.is_beinvloedbaar,
            "has_contract": len(contracts_for_supplier) > 0,
            "contract_names": contracts_for_supplier,
            "spends": spends,
            "total": total,
        })
    
    # Sort by total descending
    result.sort(key=lambda x: abs(x["total"]), reverse=True)
    
    # Paginate
    offset = (page - 1) * page_size
    total_count = len(result)
    result = result[offset:offset + page_size]
    
    return {
        "total_count": total_count,
        "page": page,
        "page_size": page_size,
        "suppliers": result,
    }


@router.get("/by-category")
async def spend_by_category(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    year: Optional[int] = None,
):
    query = (
        db.query(
            SupplierCategorization.category_id,
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("total_spend"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label("supplier_count"),
        )
        .join(SupplierYearlySpend, SupplierYearlySpend.supplier_id == SupplierCategorization.supplier_id)
        .filter(SupplierCategorization.organization_id == org_id)
    )
    if year:
        query = query.filter(SupplierYearlySpend.year == year)
    
    rows = query.group_by(
        SupplierCategorization.category_id,
        SupplierYearlySpend.year,
    ).all()
    
    return [
        {
            "category_id": r.category_id,
            "year": r.year,
            "total_spend": float(r.total_spend or 0),
            "supplier_count": r.supplier_count,
        }
        for r in rows
    ]


@router.get("/category-growth")
async def category_growth(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get year-over-year growth per category."""
    rows = (
        db.query(
            SupplierCategorization.category_id,
            InkoopCategory.inkooppakket,
            InkoopCategory.groep,
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("total_spend"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label(
                "supplier_count"
            ),
        )
        .join(
            SupplierYearlySpend,
            SupplierYearlySpend.supplier_id
            == SupplierCategorization.supplier_id,
        )
        .join(
            InkoopCategory,
            InkoopCategory.id == SupplierCategorization.category_id,
        )
        .filter(SupplierCategorization.organization_id == org_id)
        .group_by(
            SupplierCategorization.category_id,
            InkoopCategory.inkooppakket,
            InkoopCategory.groep,
            SupplierYearlySpend.year,
        )
        .order_by(InkoopCategory.inkooppakket, SupplierYearlySpend.year)
        .all()
    )

    categories: dict = defaultdict(
        lambda: {"name": "", "groep": "", "years": {}}
    )
    for r in rows:
        cat = categories[r.category_id]
        cat["id"] = r.category_id
        cat["name"] = r.inkooppakket
        cat["groep"] = r.groep
        cat["years"][r.year] = {
            "spend": float(r.total_spend or 0),
            "supplier_count": r.supplier_count,
        }

    result = []
    for cat_id, cat in categories.items():
        years_sorted = sorted(cat["years"].keys())
        growth_pct = None
        if len(years_sorted) >= 2:
            latest = cat["years"][years_sorted[-1]]["spend"]
            previous = cat["years"][years_sorted[-2]]["spend"]
            if previous > 0:
                growth_pct = round((latest - previous) / previous * 100, 1)

        result.append(
            {
                "category_id": cat_id,
                "category_name": cat["name"],
                "groep": cat["groep"],
                "years": cat["years"],
                "growth_pct": growth_pct,
                "total_spend": sum(
                    y["spend"] for y in cat["years"].values()
                ),
            }
        )

    result.sort(
        key=lambda x: x["growth_pct"]
        if x["growth_pct"] is not None
        else -999,
        reverse=True,
    )
    return result


@router.get("/new-suppliers")
async def new_suppliers(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    year: Optional[int] = None,
):
    """Detect suppliers that appeared in a given year but not in prior years."""
    if not year:
        max_year = (
            db.query(func.max(SupplierYearlySpend.year))
            .filter(SupplierYearlySpend.organization_id == org_id)
            .scalar()
        )
        year = max_year or 2025

    # Suppliers active in target year
    current_year_sids = (
        db.query(SupplierYearlySpend.supplier_id)
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year == year,
        )
        .subquery()
    )

    # Suppliers active in any prior year
    prior_year_sids = (
        db.query(SupplierYearlySpend.supplier_id)
        .filter(
            SupplierYearlySpend.organization_id == org_id,
            SupplierYearlySpend.year < year,
        )
        .subquery()
    )

    # New = in current year but NOT in any prior year
    new_sups = (
        db.query(Supplier)
        .filter(
            Supplier.organization_id == org_id,
            Supplier.id.in_(current_year_sids),
            ~Supplier.id.in_(prior_year_sids),
        )
        .all()
    )

    new_results = []
    for s in new_sups:
        spend = 0.0
        for ys in s.yearly_spends:
            if ys.year == year:
                spend = float(ys.total_amount)
                break
        cats = s.categorizations or []
        primary = max(cats, key=lambda c: c.percentage, default=None) if cats else None
        cat_name = primary.category.inkooppakket if primary and primary.category else None
        new_results.append(
            {
                "id": s.id,
                "name": s.name,
                "supplier_code": s.supplier_code,
                "spend": spend,
                "category_name": cat_name,
            }
        )
    new_results.sort(key=lambda x: -x["spend"])

    # Lost = in prior years but NOT in current year
    lost_sups = (
        db.query(Supplier)
        .filter(
            Supplier.organization_id == org_id,
            Supplier.id.in_(prior_year_sids),
            ~Supplier.id.in_(current_year_sids),
        )
        .all()
    )

    lost_results = []
    for s in lost_sups:
        # Get last known spend
        last_spend = 0.0
        for ys in sorted(s.yearly_spends, key=lambda y: y.year, reverse=True):
            if ys.year < year:
                last_spend = float(ys.total_amount)
                break
        cats = s.categorizations or []
        primary = max(cats, key=lambda c: c.percentage, default=None) if cats else None
        cat_name = primary.category.inkooppakket if primary and primary.category else None
        lost_results.append(
            {
                "id": s.id,
                "name": s.name,
                "supplier_code": s.supplier_code,
                "spend": last_spend,
                "category_name": cat_name,
            }
        )
    lost_results.sort(key=lambda x: -x["spend"])

    return {
        "year": year,
        "new_suppliers": new_results,
        "new_count": len(new_results),
        "new_total_spend": sum(r["spend"] for r in new_results),
        "lost_suppliers": lost_results,
        "lost_count": len(lost_results),
    }


class BeinvloedbaarheidUpdate(BaseModel):
    is_beinvloedbaar: bool


@router.put("/suppliers/{supplier_id}/beinvloedbaar")
async def toggle_beinvloedbaar(
    org_id: int,
    supplier_id: int,
    body: BeinvloedbaarheidUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Toggle whether a supplier is beïnvloedbaar (influenceable)."""
    supplier = (
        db.query(Supplier)
        .filter(Supplier.id == supplier_id, Supplier.organization_id == org_id)
        .first()
    )
    if not supplier:
        raise HTTPException(status_code=404, detail="Leverancier niet gevonden")
    supplier.is_beinvloedbaar = body.is_beinvloedbaar
    db.commit()
    return {"id": supplier.id, "is_beinvloedbaar": supplier.is_beinvloedbaar}


# ── Niet-beïnvloedbaar suggesties ──────────────────────────────────────

# Patronen die typisch niet-beïnvloedbare leveranciers herkennen.
# Elke tuple: (regex-patroon, reden voor de suggestie)
_NB_PATTERNS: list[tuple[str, str]] = [
    # Belastingdienst / fiscale instanties
    (r"belasting", "Belastingdienst / fiscale heffingen"),
    (r"douane", "Douane / invoerrechten"),
    # Sociale zekerheid
    (r"\buwv\b", "UWV — uitkeringen & premies"),
    (r"\bsvb\b", "SVB — sociale verzekeringen"),
    (r"\babp\b", "ABP — pensioenfonds"),
    (r"\bpfzw\b", "PFZW — pensioenfonds"),
    (r"pensioenfonds", "Pensioenfonds — verplichte afdrachten"),
    # Overheidsinstanties / registraties
    (r"kadaster", "Kadaster — registratiekosten"),
    (r"kamer van koophandel|kvk|\bkvk\b", "KvK — handelsregister"),
    (r"\bcbs\b", "CBS — Centraal Bureau voor de Statistiek"),
    (r"\brivm\b", "RIVM"),
    (r"\brdw\b", "RDW — voertuigregistratie"),
    (r"\bind\b", "IND — Immigratie en Naturalisatie"),
    (r"\bduo\b", "DUO — studiefinanciering"),
    (r"rijksoverheid", "Rijksoverheid"),
    (r"rijkswaterstaat", "Rijkswaterstaat"),
    # Waterschappen
    (r"hoogheemraadschap", "Hoogheemraadschap — waterbeheer"),
    # Gemeentelijke & provinciale heffingen
    (r"gemeente\b", "Gemeente — lokale heffingen & leges"),
    (r"provincie\b", "Provincie — provinciale heffingen"),
    # Nutsbedrijven / monopolies
    (r"netbeheer|enexis|liander|stedin|tennet|gasunie", "Netbeheerder — gereguleerde tarieven"),
    # Verzekeringen (verplicht)
    (r"zorgverzekering|zvw|\bcak\b", "Zorgverzekering / CAK — verplichte premies"),
    # Leges & rechten
    (r"leges|griffie", "Leges / griffierechten"),
    # Afvalverwerking monopolie
    (r"afvalstoffenheffing|reinigingsrecht", "Afvalstoffenheffing"),
]

# Gecompileerde patronen (case-insensitive)
_NB_COMPILED = [(re.compile(p, re.IGNORECASE), reason) for p, reason in _NB_PATTERNS]


@router.get("/suggest-niet-beinvloedbaar")
async def suggest_niet_beinvloedbaar(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Suggest suppliers that are commonly considered niet-beïnvloedbaar."""
    # Only suggest for suppliers that are currently marked as beïnvloedbaar
    suppliers = (
        db.query(Supplier)
        .filter(
            Supplier.organization_id == org_id,
            Supplier.is_beinvloedbaar == True,  # noqa: E712
        )
        .all()
    )

    suggestions = []
    for s in suppliers:
        name_lower = (s.name or "").lower()
        normalized_lower = (s.normalized_name or "").lower()
        match_text = f"{name_lower} {normalized_lower}"

        for pattern, reason in _NB_COMPILED:
            if pattern.search(match_text):
                # Get most recent spend
                latest_spend = 0.0
                for ys in sorted(s.yearly_spends, key=lambda y: y.year, reverse=True):
                    latest_spend = float(ys.total_amount)
                    break

                suggestions.append({
                    "id": s.id,
                    "name": s.name,
                    "reason": reason,
                    "total_spend": latest_spend,
                })
                break  # one match per supplier is enough

    # Sort by spend descending
    suggestions.sort(key=lambda x: -x["total_spend"])

    return {
        "suggestions": suggestions,
        "count": len(suggestions),
    }


class BulkBeinvloedbaarheidUpdate(BaseModel):
    supplier_ids: List[int]
    is_beinvloedbaar: bool


@router.put("/bulk-beinvloedbaar")
async def bulk_toggle_beinvloedbaar(
    org_id: int,
    body: BulkBeinvloedbaarheidUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Bulk update beïnvloedbaar status for multiple suppliers."""
    updated = (
        db.query(Supplier)
        .filter(
            Supplier.organization_id == org_id,
            Supplier.id.in_(body.supplier_ids),
        )
        .update(
            {Supplier.is_beinvloedbaar: body.is_beinvloedbaar},
            synchronize_session="fetch",
        )
    )
    db.commit()
    return {"updated_count": updated}


@router.get("/multi-year-trends")
async def multi_year_trends(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get multi-year spend trends at total and groep level."""
    totals = (
        db.query(
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount).label("total_spend"),
            func.count(SupplierYearlySpend.supplier_id.distinct()).label(
                "supplier_count"
            ),
        )
        .filter(SupplierYearlySpend.organization_id == org_id)
        .group_by(SupplierYearlySpend.year)
        .order_by(SupplierYearlySpend.year)
        .all()
    )

    groep_data = (
        db.query(
            InkoopCategory.groep,
            SupplierYearlySpend.year,
            func.sum(SupplierYearlySpend.total_amount * SupplierCategorization.percentage / 100.0).label("total_spend"),
        )
        .join(
            SupplierCategorization,
            SupplierCategorization.category_id == InkoopCategory.id,
        )
        .join(
            SupplierYearlySpend,
            SupplierYearlySpend.supplier_id
            == SupplierCategorization.supplier_id,
        )
        .filter(SupplierCategorization.organization_id == org_id)
        .group_by(InkoopCategory.groep, SupplierYearlySpend.year)
        .order_by(InkoopCategory.groep, SupplierYearlySpend.year)
        .all()
    )

    return {
        "yearly_totals": [
            {
                "year": r.year,
                "total_spend": float(r.total_spend),
                "supplier_count": r.supplier_count,
            }
            for r in totals
        ],
        "groep_trends": [
            {
                "groep": r.groep,
                "year": r.year,
                "total_spend": float(r.total_spend),
            }
            for r in groep_data
        ],
    }
