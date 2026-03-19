from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db, verify_org_beheerder, verify_org_membership
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.transaction import Transaction
from app.models.user import User
from app.schemas.category import CategorizationRequest, CategorizationResponse, BulkCategorizationRequest
from app.schemas.supplier import SupplierDetailResponse, SupplierResponse
from app.schemas.transaction import TransactionResponse

router = APIRouter(
    prefix="/organizations/{org_id}/suppliers",
    tags=["suppliers"],
    dependencies=[Depends(verify_org_membership)],
)


def _enrich_supplier(supplier: Supplier) -> dict:
    """Build enriched supplier dict with total_spend, category info."""
    total_spend = sum(float(ys.total_amount) for ys in supplier.yearly_spends) if supplier.yearly_spends else 0.0
    cats = supplier.categorizations or []
    # Primary category = highest percentage
    primary = max(cats, key=lambda c: c.percentage, default=None) if cats else None
    return {
        "id": supplier.id,
        "organization_id": supplier.organization_id,
        "name": supplier.name,
        "supplier_code": supplier.supplier_code,
        "normalized_name": supplier.normalized_name,
        "notes": supplier.notes,
        "created_at": supplier.created_at.isoformat() if supplier.created_at else None,
        "updated_at": supplier.updated_at.isoformat() if supplier.updated_at else None,
        "total_spend": total_spend,
        "category_id": primary.category_id if primary else None,
        "category_name": primary.category.inkooppakket if primary and primary.category else None,
        "source": primary.source if primary else None,
        "categories": [
            {
                "id": c.id,
                "category_id": c.category_id,
                "category_name": c.category.inkooppakket if c.category else None,
                "percentage": c.percentage,
                "source": c.source,
            }
            for c in cats
        ],
        "is_multi_category": len(cats) > 1,
    }


@router.get("/")
async def list_suppliers(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    search: Optional[str] = None,
    uncategorized: bool = False,
    sort_by: str = "name",
    sort_dir: str = "asc",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    query = db.query(Supplier).filter(Supplier.organization_id == org_id)
    if search:
        # Escape LIKE wildcards to prevent wildcard injection
        safe_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = query.filter(Supplier.name.ilike(f"%{safe_search}%", escape="\\"))
    if uncategorized:
        query = query.filter(~Supplier.categorizations.any())

    # Default sort by name in DB
    query = query.order_by(Supplier.name.asc() if sort_dir == "asc" else Supplier.name.desc())

    offset = (page - 1) * page_size
    suppliers = query.offset(offset).limit(page_size).all()

    enriched = [_enrich_supplier(s) for s in suppliers]

    # Sort by total_spend if requested (done in Python since it's a computed field)
    if sort_by == "total_spend":
        enriched.sort(key=lambda x: x["total_spend"], reverse=(sort_dir == "desc"))

    return enriched


@router.get("/{supplier_id}", response_model=SupplierDetailResponse)
async def get_supplier(
    org_id: int,
    supplier_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    supplier = (
        db.query(Supplier)
        .filter(Supplier.id == supplier_id, Supplier.organization_id == org_id)
        .first()
    )
    if not supplier:
        raise HTTPException(status_code=404, detail="Leverancier niet gevonden")
    return supplier


@router.get("/{supplier_id}/transactions", response_model=list[TransactionResponse])
async def get_supplier_transactions(
    org_id: int,
    supplier_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    year: Optional[int] = None,
):
    query = db.query(Transaction).filter(
        Transaction.supplier_id == supplier_id,
        Transaction.organization_id == org_id,
    )
    if year:
        query = query.filter(Transaction.year == year)
    return query.order_by(Transaction.booking_date.desc()).all()


@router.put("/{supplier_id}/category", dependencies=[Depends(verify_org_beheerder)], response_model=CategorizationResponse)
async def set_supplier_category(
    org_id: int,
    supplier_id: int,
    data: CategorizationRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    supplier = (
        db.query(Supplier)
        .filter(Supplier.id == supplier_id, Supplier.organization_id == org_id)
        .first()
    )
    if not supplier:
        raise HTTPException(status_code=404, detail="Leverancier niet gevonden")

    # Delete all existing categorizations (scoped to org_id for safety)
    db.query(SupplierCategorization).filter(
        SupplierCategorization.supplier_id == supplier_id,
        SupplierCategorization.organization_id == org_id,
    ).delete(synchronize_session="fetch")

    cat = SupplierCategorization(
        organization_id=org_id,
        supplier_id=supplier_id,
        category_id=data.category_id,
        percentage=100.0,
        source=data.source,
        confidence=data.confidence,
        ai_reasoning=data.ai_reasoning,
        categorized_by=current_user.id,
    )
    db.add(cat)
    db.commit()
    db.refresh(cat)

    # Auto-populate master DB
    try:
        from app.services.supplier_master_service import SupplierMasterService
        master_svc = SupplierMasterService(db)
        master_svc.record_categorization(supplier, cat)
        db.commit()
    except Exception:
        pass

    return cat


@router.post("/bulk-categorize", dependencies=[Depends(verify_org_beheerder)], response_model=list[CategorizationResponse])
async def bulk_categorize(
    org_id: int,
    data: BulkCategorizationRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    results = []
    for item in data.categorizations:
        # Delete all existing categorizations for this supplier (scoped to org_id)
        db.query(SupplierCategorization).filter(
            SupplierCategorization.supplier_id == item.supplier_id,
            SupplierCategorization.organization_id == org_id,
        ).delete(synchronize_session="fetch")

        cat = SupplierCategorization(
            organization_id=org_id,
            supplier_id=item.supplier_id,
            category_id=item.category_id,
            percentage=100.0,
            source=item.source,
            categorized_by=current_user.id,
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
        for cat in results:
            supplier = db.query(Supplier).get(cat.supplier_id)
            if supplier:
                master_svc.record_categorization(supplier, cat)
        db.commit()
    except Exception:
        pass

    return results
