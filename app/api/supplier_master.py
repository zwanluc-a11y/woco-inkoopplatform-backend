"""
Supplier Master Database API.

Platform-level endpoints for managing the cross-organization
supplier → PIANOo category knowledge base.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db, verify_platform_eigenaar
from app.models.category import InkoopCategory
from app.models.supplier_master_category import SupplierMasterCategory
from app.models.user import User
from app.services.import_service import normalize_supplier_name
from app.services.supplier_master_service import SupplierMasterService, suggest_category_for_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/supplier-master", tags=["supplier-master"])


# ── Pydantic schemas ────────────────────────────────────────────────

class CreateMasterEntryRequest(BaseModel):
    supplier_name: str
    category_id: int
    notes: Optional[str] = None


class UpdateMasterEntryRequest(BaseModel):
    display_name: Optional[str] = None
    category_id: Optional[int] = None
    notes: Optional[str] = None


class BulkLookupRequest(BaseModel):
    normalized_names: list[str]


class SuggestCategoryRequest(BaseModel):
    supplier_names: list[str]


class BulkCategorizeRequest(BaseModel):
    """Apply AI-suggested categories to multiple entries at once."""
    assignments: list[dict]  # [{"entry_id": int, "category_id": int}, ...]


def _serialize(entry) -> dict:
    return {
        "id": entry.id,
        "normalized_name": entry.normalized_name,
        "display_name": entry.display_name,
        "category_id": entry.category_id,
        "category_nummer": entry.category_nummer,
        "category_name": entry.category_name,
        "category_system": entry.category_system,
        "usage_count": entry.usage_count,
        "source": entry.source,
        "notes": entry.notes,
        "created_at": entry.created_at.isoformat() if entry.created_at else None,
        "updated_at": entry.updated_at.isoformat() if entry.updated_at else None,
    }


# ── List / Search (platform users) ─────────────────────────────────

@router.get("")
def list_master_entries(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    search: str = "",
    category_system: Optional[str] = Query(None, description="Categoriesysteem filter"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """List/search all master database entries (paginated)."""
    service = SupplierMasterService(db)
    entries, total = service.search(search, page, page_size, category_system=category_system)
    return {
        "entries": [_serialize(e) for e in entries],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ── Stats (platform users) ─────────────────────────────────────────

@router.get("/stats")
def master_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    category_system: Optional[str] = Query(None, description="Categoriesysteem filter"),
):
    """Get aggregate statistics for the master database."""
    service = SupplierMasterService(db)
    return service.get_stats(category_system=category_system)


# ── Create (platform users) ────────────────────────────────────────

@router.post("")
def create_master_entry(
    req: CreateMasterEntryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """Manually add a new supplier-category mapping."""
    category = db.query(InkoopCategory).get(req.category_id)
    if not category:
        raise HTTPException(status_code=404, detail="Categorie niet gevonden")

    normalized = normalize_supplier_name(req.supplier_name)
    if not normalized:
        raise HTTPException(status_code=400, detail="Ongeldige leveranciersnaam")

    service = SupplierMasterService(db)
    entry = service.upsert(
        normalized_name=normalized,
        display_name=req.supplier_name.strip(),
        category_id=category.id,
        category_nummer=category.nummer,
        category_name=category.inkooppakket,
        source="manual",
    )
    if req.notes:
        entry.notes = req.notes
    db.commit()
    db.refresh(entry)
    return _serialize(entry)


# ── Update (platform users) ────────────────────────────────────────

@router.put("/{entry_id}")
def update_master_entry(
    entry_id: int,
    req: UpdateMasterEntryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """Update an existing master entry."""
    service = SupplierMasterService(db)
    entry = service.update_entry(
        entry_id,
        category_id=req.category_id,
        notes=req.notes,
        display_name=req.display_name,
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Entry niet gevonden")
    db.commit()
    db.refresh(entry)
    return _serialize(entry)


# ── Delete (platform users) ────────────────────────────────────────

@router.delete("/{entry_id}")
def delete_master_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """Delete a single master entry."""
    service = SupplierMasterService(db)
    if not service.delete_entry(entry_id):
        raise HTTPException(status_code=404, detail="Entry niet gevonden")
    db.commit()
    return {"ok": True}


@router.post("/clear-all")
def clear_master_db(
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """Delete ALL master database entries. Used to reset and rebuild from scratch."""
    count = db.query(SupplierMasterCategory).count()
    db.query(SupplierMasterCategory).delete()
    db.commit()
    logger.info("Cleared master DB: deleted %d entries", count)
    return {"deleted": count, "message": f"{count} entries verwijderd uit Master Database"}


# ── CSV Import (platform users) ────────────────────────────────────

@router.post("/import-csv")
def import_csv(
    file: UploadFile = File(...),
    category_system: str = Query("woco", description="Categoriesysteem filter"),
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """Upload a CSV file with supplier_name and category_nummer columns."""
    content = file.file.read()
    # Limit CSV file size to 5MB
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="CSV bestand is te groot. Maximum is 5 MB.")
    service = SupplierMasterService(db)
    return service.bulk_upsert_from_csv(content, category_system=category_system)


# ── Backfill from existing categorizations (platform users) ────────

@router.post("/backfill")
def backfill_from_existing(
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """One-time backfill: import all confirmed categorizations into master DB."""
    from app.models.supplier import Supplier
    from app.models.supplier_categorization import SupplierCategorization

    try:
        categorizations = (
            db.query(SupplierCategorization)
            .join(Supplier, Supplier.id == SupplierCategorization.supplier_id)
            .filter(
                SupplierCategorization.source.in_(
                    ["manual", "ai_confirmed", "ai_accepted", "imported"]
                )
            )
            .all()
        )
    except Exception as e:
        logger.error("Backfill: fout bij ophalen categorisaties: %s", e)
        raise HTTPException(status_code=500, detail="Fout bij ophalen categorisaties")

    # Pre-load all suppliers and categories to avoid N+1 queries
    supplier_ids = {cat.supplier_id for cat in categorizations}
    category_ids = {cat.category_id for cat in categorizations}

    suppliers_by_id = {
        s.id: s
        for s in db.query(Supplier).filter(Supplier.id.in_(supplier_ids)).all()
    } if supplier_ids else {}

    categories_by_id = {
        c.id: c
        for c in db.query(InkoopCategory).filter(InkoopCategory.id.in_(category_ids)).all()
    } if category_ids else {}

    # Pre-load existing master entries to avoid duplicates
    existing_keys: set[tuple[str, int]] = set()
    existing_entries = db.query(SupplierMasterCategory).all()
    entry_lookup: dict[tuple[str, int], SupplierMasterCategory] = {}
    for e in existing_entries:
        key = (e.normalized_name, e.category_id)
        existing_keys.add(key)
        entry_lookup[key] = e

    created = 0
    updated = 0
    skipped = 0

    for cat in categorizations:
        supplier = suppliers_by_id.get(cat.supplier_id)
        if not supplier or not supplier.normalized_name:
            skipped += 1
            continue

        category = categories_by_id.get(cat.category_id)
        if not category:
            skipped += 1
            continue

        key = (supplier.normalized_name, category.id)
        if key in existing_keys:
            entry_lookup[key].usage_count += 1
            updated += 1
        else:
            entry = SupplierMasterCategory(
                normalized_name=supplier.normalized_name,
                display_name=supplier.name,
                category_id=category.id,
                category_nummer=category.nummer,
                category_name=category.inkooppakket,
                category_system=category.category_system or "woco",
                usage_count=1,
                source="auto",
            )
            db.add(entry)
            existing_keys.add(key)
            entry_lookup[key] = entry
            created += 1

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Backfill: fout bij commit: %s", e)
        raise HTTPException(status_code=500, detail="Fout bij opslaan")

    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "total_processed": len(categorizations),
    }


# ── Bulk Lookup (any authenticated user) ───────────────────────────

@router.post("/bulk-lookup")
def bulk_lookup(
    req: BulkLookupRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Batch lookup for categorization page. Returns matches per normalized name."""
    service = SupplierMasterService(db)
    results = service.bulk_lookup(req.normalized_names)
    return {
        name: [_serialize(e) for e in entries]
        for name, entries in results.items()
    }


# ── AI Category Suggestions ──────────────────────────────────────────

@router.post("/suggest-categories")
def suggest_categories(
    req: SuggestCategoryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    category_system: str = Query("woco", description="Categoriesysteem"),
):
    """
    Suggest WoCo categories for a list of supplier names using AI keyword matching.
    Returns a dict: { "supplier_name": { category_nummer, category_naam, category_id, confidence } }
    """
    # Build category lookup
    cats = (
        db.query(InkoopCategory)
        .filter(InkoopCategory.category_system == category_system)
        .all()
    )
    cats_by_nummer = {
        c.nummer: {"inkooppakket": c.inkooppakket, "id": c.id}
        for c in cats
    }

    results = {}
    for name in req.supplier_names:
        suggestion = suggest_category_for_name(name, cats_by_nummer)
        if suggestion:
            results[name] = suggestion
        else:
            results[name] = None

    return results


@router.post("/bulk-categorize")
def bulk_categorize(
    req: BulkCategorizeRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_eigenaar),
):
    """
    Apply category assignments to multiple supplier master entries at once.
    Each assignment: { entry_id: int, category_id: int }
    """
    service = SupplierMasterService(db)

    # Pre-load all categories
    cat_ids = {a["category_id"] for a in req.assignments if a.get("category_id")}
    categories_by_id = {
        c.id: c
        for c in db.query(InkoopCategory).filter(InkoopCategory.id.in_(cat_ids)).all()
    } if cat_ids else {}

    updated = 0
    skipped = 0
    errors = []

    for assignment in req.assignments:
        entry_id = assignment.get("entry_id")
        category_id = assignment.get("category_id")

        if not entry_id or not category_id:
            skipped += 1
            continue

        category = categories_by_id.get(category_id)
        if not category:
            errors.append(f"Categorie {category_id} niet gevonden")
            continue

        entry = service.update_entry(
            entry_id,
            category_id=category.id,
        )
        if entry:
            updated += 1
        else:
            errors.append(f"Entry {entry_id} niet gevonden")

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
    }
