"""Categorization API endpoints."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)

from app.api.deps import get_current_user, get_db, verify_org_beheerder, verify_org_membership
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.user import User
from app.services.categorization_service import CategorizationService

router = APIRouter(
    prefix="/organizations/{org_id}/categorization",
    tags=["categorization"],
    dependencies=[Depends(verify_org_membership)],
)


class AISuggestRequest(BaseModel):
    supplier_ids: Optional[list[int]] = None
    batch_size: int = 5


class AISuggestResponse(BaseModel):
    suggestions: list[dict]
    count: int


class SetCategoryRequest(BaseModel):
    category_id: int
    source: str = "manual"


class MultiCategoryItem(BaseModel):
    category_id: int
    percentage: float


class SetMultiCategoriesRequest(BaseModel):
    categories: list[MultiCategoryItem]


class BulkAcceptRequest(BaseModel):
    supplier_ids: list[int]


class BulkRejectRequest(BaseModel):
    supplier_ids: list[int]


@router.get("/status")
def get_categorization_status(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get categorization progress for the organization."""
    service = CategorizationService(db)
    return service.get_status(org_id)


@router.get("/debug")
def debug_categorization(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Temporary debug endpoint to diagnose AI categorization issues."""
    from sqlalchemy import text, exists

    results = {"version": "v7-debug-endpoint"}

    try:
        # 1. Diagnostics via service
        service = CategorizationService(db)
        results["diagnostics"] = service.get_uncategorized_counts(org_id)
    except Exception as e:
        results["diagnostics_error"] = str(e)

    try:
        # 2. NOT EXISTS query test (same as ai_categorize_batch uses)
        categorized_exists = (
            db.query(SupplierCategorization.id)
            .filter(
                SupplierCategorization.supplier_id == Supplier.id,
                SupplierCategorization.organization_id == org_id,
            )
            .correlate(Supplier)
            .exists()
        )
        query = db.query(Supplier).filter(
            Supplier.organization_id == org_id,
            ~categorized_exists,
        )
        results["not_exists_count"] = query.count()
        first_5 = query.limit(5).all()
        results["not_exists_all_len"] = len(first_5)
        results["first_5_ids"] = [s.id for s in first_5]
    except Exception as e:
        results["not_exists_error"] = str(e)

    try:
        # 3. Raw SQL test (bypasses SQLAlchemy ORM completely)
        raw = db.execute(text(
            "SELECT COUNT(*) FROM suppliers s "
            "WHERE s.organization_id = :org "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM supplier_categorizations sc "
            "  WHERE sc.supplier_id = s.id AND sc.organization_id = :org"
            ")"
        ), {"org": org_id}).scalar()
        results["raw_sql_count"] = raw
    except Exception as e:
        results["raw_sql_error"] = str(e)

    try:
        # 4. Even simpler: total suppliers vs categorized
        total = db.query(Supplier).filter(Supplier.organization_id == org_id).count()
        categorized = db.execute(text(
            "SELECT COUNT(DISTINCT supplier_id) FROM supplier_categorizations "
            "WHERE organization_id = :org"
        ), {"org": org_id}).scalar()
        results["total_suppliers"] = total
        results["categorized_distinct"] = categorized
        results["uncategorized_simple"] = total - (categorized or 0)
    except Exception as e:
        results["simple_count_error"] = str(e)

    try:
        # 5. API key check
        from app.api.settings import get_anthropic_api_key
        key = get_anthropic_api_key(db)
        results["api_key_exists"] = bool(key)
        results["api_key_length"] = len(key) if key else 0
    except Exception as e:
        results["api_key_error"] = str(e)

    return results


@router.post("/auto-match-master", dependencies=[Depends(verify_org_beheerder)])
def auto_match_master_db(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Auto-categorize uncategorized suppliers using the Master Database.

    Finds all uncategorized suppliers, looks them up in the cross-org
    master DB, and auto-applies matches with source='master_db'.
    """
    from sqlalchemy import exists
    from app.services.supplier_master_service import SupplierMasterService

    # Get uncategorized suppliers using NOT EXISTS (more reliable than NOT IN)
    categorized_exists = (
        db.query(SupplierCategorization.id)
        .filter(
            SupplierCategorization.supplier_id == Supplier.id,
            SupplierCategorization.organization_id == org_id,
        )
        .correlate(Supplier)
        .exists()
    )
    uncategorized = (
        db.query(Supplier)
        .filter(
            Supplier.organization_id == org_id,
            ~categorized_exists,
        )
        .all()
    )

    if not uncategorized:
        return {"matched": 0, "results": []}

    master_svc = SupplierMasterService(db)
    normalized_names = [s.normalized_name for s in uncategorized if s.normalized_name]
    master_lookups = master_svc.bulk_lookup(normalized_names)

    results = []
    for s in uncategorized:
        matches = master_lookups.get(s.normalized_name, []) if s.normalized_name else []
        if not matches:
            continue

        top_match = matches[0]
        categorization = SupplierCategorization(
            organization_id=org_id,
            supplier_id=s.id,
            category_id=top_match.category_id,
            percentage=100.0,
            source="master_db",
            confidence=1.0,
            ai_reasoning=(
                f"Automatisch gekoppeld via Master Database "
                f"(gebruikt bij {top_match.usage_count} organisatie(s))"
            ),
        )
        db.add(categorization)
        top_match.usage_count += 1

        results.append({
            "supplier_id": s.id,
            "supplier_name": s.name,
            "category_id": top_match.category_id,
            "category_name": top_match.category_name,
            "category_nummer": top_match.category_nummer,
        })

    if results:
        db.commit()

    return {"matched": len(results), "results": results}


@router.post("/ai-suggest", dependencies=[Depends(verify_org_beheerder)])
@limiter.limit("5/minute")
def run_ai_categorization(
    request: Request,  # required by slowapi
    org_id: int,
    data: AISuggestRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Run AI categorization for uncategorized suppliers."""
    service = CategorizationService(db)
    try:
        suggestions, diagnostics = service.ai_categorize_batch(
            org_id=org_id,
            supplier_ids=data.supplier_ids,
            batch_size=data.batch_size,
        )
        return {
            "suggestions": suggestions,
            "count": len(suggestions),
            "diagnostics": diagnostics,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("AI categorization failed for org %d", org_id)
        raise HTTPException(status_code=500, detail=f"Interne fout: {type(e).__name__}: {str(e)}")


@router.get("/suggestions")
def get_pending_suggestions(
    org_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get all pending AI suggestions for review, ordered by spend DESC."""
    service = CategorizationService(db)
    suggestions = service.get_suggestions(org_id)

    results = []
    for s in suggestions:
        supplier = db.query(Supplier).get(s.supplier_id)
        # Calculate total spend for this supplier
        total_spend = (
            db.query(func.sum(SupplierYearlySpend.total_amount))
            .filter(SupplierYearlySpend.supplier_id == s.supplier_id)
            .scalar()
            or 0
        )
        results.append(
            {
                "id": s.id,
                "supplier_id": s.supplier_id,
                "supplier_name": supplier.name if supplier else "Onbekend",
                "category_id": s.category_id,
                "category_name": s.category.inkooppakket if s.category else None,
                "category_nummer": s.category.nummer if s.category else None,
                "confidence": s.confidence,
                "reasoning": s.ai_reasoning,
                "source": s.source,
                "total_spend": float(total_spend),
            }
        )

    return results


@router.post("/bulk-accept", dependencies=[Depends(verify_org_beheerder)])
def bulk_accept_suggestions(
    org_id: int,
    request: BulkAcceptRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Accept multiple AI suggestions at once."""
    service = CategorizationService(db)
    results = service.bulk_accept(org_id, request.supplier_ids, user.id)
    return {"accepted": len(results), "results": results}


@router.post("/bulk-reject", dependencies=[Depends(verify_org_beheerder)])
def bulk_reject_suggestions(
    org_id: int,
    request: BulkRejectRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Reject multiple AI suggestions (delete the ai_suggested categorizations)."""
    service = CategorizationService(db)
    count = service.bulk_reject(org_id, request.supplier_ids)
    return {"rejected": count}


@router.put("/suppliers/{supplier_id}/category", dependencies=[Depends(verify_org_beheerder)])
def set_supplier_category(
    org_id: int,
    supplier_id: int,
    request: SetCategoryRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Set or update a supplier's PIANOo category."""
    service = CategorizationService(db)
    try:
        result = service.set_category(
            org_id=org_id,
            supplier_id=supplier_id,
            category_id=request.category_id,
            source=request.source,
            user_id=user.id,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {
        "id": result.id,
        "supplier_id": result.supplier_id,
        "category_id": result.category_id,
        "percentage": result.percentage,
        "source": result.source,
        "confidence": result.confidence,
    }


@router.put("/suppliers/{supplier_id}/multi-category", dependencies=[Depends(verify_org_beheerder)])
def set_supplier_multi_categories(
    org_id: int,
    supplier_id: int,
    request: SetMultiCategoriesRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Assign multiple PIANOo categories to a supplier with percentage split."""
    # Validate supplier exists
    supplier = db.query(Supplier).filter(
        Supplier.id == supplier_id, Supplier.organization_id == org_id
    ).first()
    if not supplier:
        raise HTTPException(status_code=404, detail="Leverancier niet gevonden")

    if not request.categories:
        raise HTTPException(status_code=400, detail="Minimaal 1 categorie vereist")

    # Validate no duplicate categories
    cat_ids = [c.category_id for c in request.categories]
    if len(cat_ids) != len(set(cat_ids)):
        raise HTTPException(status_code=400, detail="Dubbele categorie gevonden")

    # Validate percentages sum to 100
    total_pct = sum(c.percentage for c in request.categories)
    if abs(total_pct - 100.0) > 0.01:
        raise HTTPException(
            status_code=400,
            detail=f"Percentages moeten optellen tot 100% (huidig: {total_pct:.1f}%)",
        )

    # Delete all existing categorizations (scoped to org_id for safety)
    db.query(SupplierCategorization).filter(
        SupplierCategorization.supplier_id == supplier_id,
        SupplierCategorization.organization_id == org_id,
    ).delete(synchronize_session="fetch")

    # Create new categorizations
    results = []
    for item in request.categories:
        cat = SupplierCategorization(
            organization_id=org_id,
            supplier_id=supplier_id,
            category_id=item.category_id,
            percentage=item.percentage,
            source="manual",
            categorized_by=user.id,
        )
        db.add(cat)
        results.append(cat)

    db.commit()
    for r in results:
        db.refresh(r)

    # Auto-populate master DB
    try:
        from app.services.supplier_master_service import SupplierMasterService
        master_svc = SupplierMasterService(db)
        for r in results:
            master_svc.record_categorization(supplier, r)
        db.commit()
    except Exception:
        pass

    return [
        {
            "id": r.id,
            "supplier_id": r.supplier_id,
            "category_id": r.category_id,
            "percentage": r.percentage,
            "category_name": r.category.inkooppakket if r.category else None,
            "source": r.source,
        }
        for r in results
    ]
