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

from app.api.deps import get_current_user, get_db, verify_platform_user
from app.models.category import InkoopCategory
from app.models.supplier_master_category import SupplierMasterCategory
from app.models.user import User
from app.services.import_service import normalize_supplier_name
from app.services.supplier_master_service import SupplierMasterService

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


def _serialize(entry) -> dict:
    return {
        "id": entry.id,
        "normalized_name": entry.normalized_name,
        "display_name": entry.display_name,
        "category_id": entry.category_id,
        "category_nummer": entry.category_nummer,
        "category_name": entry.category_name,
        "category_system": "aedes",
        "usage_count": entry.usage_count,
        "source": entry.source,
        "notes": entry.notes,
        "created_at": entry.created_at.isoformat() if entry.created_at else None,
        "updated_at": entry.updated_at.isoformat() if entry.updated_at else None,
    }


# ── List / Search (platform users) ─────────────────────────────────

@router.get("/")
async def list_master_entries(
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
    search: str = "",
    category_system: Optional[str] = Query(None, description="Categoriesysteem filter"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """List/search all master database entries (paginated)."""
    service = SupplierMasterService(db)
    entries, total = service.search(search, page, page_size)
    return {
        "entries": [_serialize(e) for e in entries],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ── Stats (platform users) ─────────────────────────────────────────

@router.get("/stats")
async def master_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
    category_system: Optional[str] = Query(None, description="Categoriesysteem filter"),
):
    """Get aggregate statistics for the master database."""
    service = SupplierMasterService(db)
    return service.get_stats()


# ── Create (platform users) ────────────────────────────────────────

@router.post("/")
async def create_master_entry(
    req: CreateMasterEntryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
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
async def update_master_entry(
    entry_id: int,
    req: UpdateMasterEntryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
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
async def delete_master_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
):
    """Delete a single master entry."""
    service = SupplierMasterService(db)
    if not service.delete_entry(entry_id):
        raise HTTPException(status_code=404, detail="Entry niet gevonden")
    db.commit()
    return {"ok": True}


# ── CSV Import (platform users) ────────────────────────────────────

@router.post("/import-csv")
async def import_csv(
    file: UploadFile = File(...),
    category_system: str = Query("aedes", description="Categoriesysteem filter"),
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
):
    """Upload a CSV file with supplier_name and category_nummer columns."""
    content = await file.read()
    # Limit CSV file size to 5MB
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="CSV bestand is te groot. Maximum is 5 MB.")
    service = SupplierMasterService(db)
    return service.bulk_upsert_from_csv(content)


# ── Backfill from existing categorizations (platform users) ────────

@router.post("/backfill")
async def backfill_from_existing(
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_platform_user),
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
                category_system="aedes",
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
async def bulk_lookup(
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
