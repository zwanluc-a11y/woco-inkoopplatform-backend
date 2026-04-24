"""API for managing departments (afdelingen) linked to inkoop categories."""
from __future__ import annotations

from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.category import InkoopCategory
from app.models.category_department import CategoryDepartment
from app.models.user import User

router = APIRouter(
    prefix="/organizations/{org_id}/departments",
    tags=["departments"],
)


class CategoryDepartmentUpdate(BaseModel):
    category_id: int
    afdeling: Optional[str] = Field(default=None)


class BulkDepartmentUpdate(BaseModel):
    mappings: List[CategoryDepartmentUpdate]


@router.get("")
def list_departments(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """List all unique departments used by this organization."""
    rows = (
        db.query(CategoryDepartment.afdeling)
        .filter(CategoryDepartment.organization_id == org_id)
        .distinct()
        .all()
    )
    departments = sorted([r.afdeling for r in rows if r.afdeling])
    return {"departments": departments, "count": len(departments)}


@router.get("/mappings")
def list_category_mappings(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """List all category → department mappings for this organization."""
    mappings = (
        db.query(CategoryDepartment)
        .filter(CategoryDepartment.organization_id == org_id)
        .all()
    )

    # Build full list of all categories with their department (or null)
    all_categories = db.query(InkoopCategory).all()
    mapping_by_cat = {m.category_id: m.afdeling for m in mappings}

    result = []
    for cat in all_categories:
        result.append({
            "category_id": cat.id,
            "category_name": cat.inkooppakket,
            "groep": cat.groep,
            "afdeling": mapping_by_cat.get(cat.id),
        })

    # Sort: first by afdeling (with null last), then by name
    result.sort(key=lambda x: (x["afdeling"] is None, x["afdeling"] or "", x["category_name"]))
    return {
        "mappings": result,
        "total_count": len(result),
        "mapped_count": sum(1 for r in result if r["afdeling"]),
    }


@router.put("/mappings")
def update_category_mapping(
    org_id: int,
    body: CategoryDepartmentUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Set or clear the department for a single category."""
    cat = db.query(InkoopCategory).filter(InkoopCategory.id == body.category_id).first()
    if not cat:
        raise HTTPException(status_code=404, detail="Categorie niet gevonden")

    existing = (
        db.query(CategoryDepartment)
        .filter(
            CategoryDepartment.organization_id == org_id,
            CategoryDepartment.category_id == body.category_id,
        )
        .first()
    )

    afd = (body.afdeling or "").strip() or None

    if afd is None:
        # Clear: delete the mapping if it exists
        if existing:
            db.delete(existing)
            db.commit()
        return {"category_id": body.category_id, "afdeling": None}

    if existing:
        existing.afdeling = afd
    else:
        existing = CategoryDepartment(
            organization_id=org_id,
            category_id=body.category_id,
            afdeling=afd,
        )
        db.add(existing)

    db.commit()
    return {"category_id": body.category_id, "afdeling": afd}


@router.put("/mappings/bulk")
def bulk_update_mappings(
    org_id: int,
    body: BulkDepartmentUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Bulk set departments for multiple categories at once."""
    updated = 0
    cleared = 0
    for m in body.mappings:
        existing = (
            db.query(CategoryDepartment)
            .filter(
                CategoryDepartment.organization_id == org_id,
                CategoryDepartment.category_id == m.category_id,
            )
            .first()
        )
        afd = (m.afdeling or "").strip() or None

        if afd is None:
            if existing:
                db.delete(existing)
                cleared += 1
            continue

        if existing:
            existing.afdeling = afd
        else:
            db.add(
                CategoryDepartment(
                    organization_id=org_id,
                    category_id=m.category_id,
                    afdeling=afd,
                )
            )
        updated += 1
    db.commit()
    return {"updated": updated, "cleared": cleared}


# Default department mapping (from Eigen Haard Categoriemonitor)
# Used to pre-populate mappings based on category name match.
DEFAULT_CATEGORY_DEPARTMENT_MAP: dict[str, str] = {
    "Afval": "Vastgoed",
    "Asbest": "Vastgoed",
    "Automatische deuren": "Vastgoed",
    "BKT": "Vastgoed",
    "Brandbeveiliging - BMI": "Vastgoed",
    "Brandbeveiliging - blusmiddelen en nv": "Vastgoed",
    "Brandbeveiliging": "Vastgoed",
    "CV Collectief": "Vastgoed",
    "CV Individueel": "Vastgoed",
    "Calamiteitenpartners": "Vastgoed",
    "Camerabeveiliging": "Wonen en Leefbaarheid",
    "Conditiemeting": "Vastgoed",
    "Dakonderhoud": "Vastgoed",
    "Elektrotechniek installateurs onderhoud": "Vastgoed",
    "Energielabels": "Vastgoed",
    "Glas": "Vastgoed",
    "Groenonderhoud": "Vastgoed",
    "Hydroforen": "Vastgoed",
    "Ketenpartners": "Vastgoed",
    "Klimaatinstallaties": "Vastgoed",
    "Kozijnen": "Vastgoed",
    "Legionella": "Vastgoed",
    "Liften": "Vastgoed",
    "MV en WTW": "Vastgoed",
    "Meetapparatuur": "Vastgoed",
    "NOM Woningen": "Vastgoed",
    "Overige aannemer": "Vastgoed",
    "Overige beheerdiensten": "Vastgoed",
    "Riolering": "Vastgoed",
    "Schoonmaak": "Wonen en Leefbaarheid",
    "Verlichting": "Vastgoed",
    "Verzekering": "Bedrijfsvoering",
    "WKO": "Vastgoed",
    "Zendmasten": "Vastgoed",
    "Zonnepanelen": "Vastgoed",
    "Zonwering": "Vastgoed",
}


@router.post("/seed-defaults")
def seed_default_mappings(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    overwrite: bool = False,
):
    """Seed default category → department mappings based on the Categoriemonitor reference.

    Matches on category name (case-insensitive, trimmed). Skips categories that
    already have a mapping unless overwrite=True.
    """
    all_categories = db.query(InkoopCategory).all()
    # Normalize default map (lowercase for lookup)
    default_lower = {k.lower().strip(): v for k, v in DEFAULT_CATEGORY_DEPARTMENT_MAP.items()}

    existing_mappings = {
        m.category_id: m
        for m in db.query(CategoryDepartment)
        .filter(CategoryDepartment.organization_id == org_id)
        .all()
    }

    matched = 0
    added = 0
    updated = 0
    skipped = 0

    for cat in all_categories:
        cat_name_lower = (cat.inkooppakket or "").lower().strip()
        afd = default_lower.get(cat_name_lower)
        if not afd:
            continue
        matched += 1
        existing = existing_mappings.get(cat.id)
        if existing:
            if overwrite:
                existing.afdeling = afd
                updated += 1
            else:
                skipped += 1
        else:
            db.add(
                CategoryDepartment(
                    organization_id=org_id,
                    category_id=cat.id,
                    afdeling=afd,
                )
            )
            added += 1

    db.commit()
    return {
        "matched": matched,
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "total_categories": len(all_categories),
    }
