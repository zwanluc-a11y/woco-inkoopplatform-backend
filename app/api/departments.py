"""API for managing departments (afdelingen) linked to inkoop categories."""
from __future__ import annotations

from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.category import InkoopCategory
from app.models.category_department import CategoryDepartment
from app.models.supplier_categorization import SupplierCategorization
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
    only_in_use: bool = True,
):
    """List category → department mappings for this organization.

    By default only returns categories actually used by the organization
    (i.e. with at least one supplier categorized to it). Set only_in_use=False
    to see the full Aedes/Woco taxonomy.
    """
    mappings = (
        db.query(CategoryDepartment)
        .filter(CategoryDepartment.organization_id == org_id)
        .all()
    )
    mapping_by_cat = {m.category_id: m.afdeling for m in mappings}

    # Get categories actually in use by this org
    in_use_ids = {
        r.category_id
        for r in db.query(SupplierCategorization.category_id)
        .filter(SupplierCategorization.organization_id == org_id)
        .distinct()
        .all()
    }

    # Count suppliers per category for context
    supplier_counts: dict[int, int] = {}
    for row in (
        db.query(
            SupplierCategorization.category_id,
            SupplierCategorization.supplier_id,
        )
        .filter(SupplierCategorization.organization_id == org_id)
        .distinct()
        .all()
    ):
        supplier_counts[row.category_id] = supplier_counts.get(row.category_id, 0) + 1

    if only_in_use:
        all_categories = (
            db.query(InkoopCategory)
            .filter(InkoopCategory.id.in_(in_use_ids))
            .all()
        ) if in_use_ids else []
    else:
        all_categories = db.query(InkoopCategory).all()

    result = []
    for cat in all_categories:
        result.append({
            "category_id": cat.id,
            "category_name": cat.inkooppakket,
            "groep": cat.groep,
            "afdeling": mapping_by_cat.get(cat.id),
            "supplier_count": supplier_counts.get(cat.id, 0),
            "in_use": cat.id in in_use_ids,
        })

    # Sort: first by afdeling (with null last), then by name
    result.sort(key=lambda x: (x["afdeling"] is None, x["afdeling"] or "", x["category_name"]))
    return {
        "mappings": result,
        "total_count": len(result),
        "mapped_count": sum(1 for r in result if r["afdeling"]),
        "in_use_count": len(in_use_ids),
        "all_categories_count": db.query(InkoopCategory).count(),
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


# Default department mapping by GROUP (covers all 119 Woco categories)
# Maps the `groep` field of each category to a default department.
DEFAULT_GROUP_DEPARTMENT_MAP: dict[str, str] = {
    "1-Vastgoed": "Vastgoed",
    "2-Installaties": "Vastgoed",
    "3-Veiligheid": "Vastgoed",
    "7-Energie": "Vastgoed",
    "12-Vastgoedbeheer": "Vastgoed",
    "11-Sociaal": "Wonen en Leefbaarheid",
    "4-Facilitair": "Bedrijfsvoering",
    "5-ICT": "Bedrijfsvoering",
    "6-Advies": "Bedrijfsvoering",
    "8-Personeel": "Bedrijfsvoering",
    "9-Financieel": "Bedrijfsvoering",
    "10-Communicatie": "Bedrijfsvoering",
}


# Default department mapping (from Eigen Haard Categoriemonitor)
# Used to pre-populate mappings based on category name match. Overrides group default.
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
    only_in_use: bool = True,
):
    """Seed default category → department mappings.

    Strategy:
      1. First try a name-based match (specific Eigen Haard mapping).
      2. Otherwise, fall back to a `groep`-based mapping that covers all 119 categories.

    By default only seeds categories actually in use by the organization.
    Set only_in_use=False to seed every category in the Woco taxonomy.
    Skips categories that already have a mapping unless overwrite=True.
    """
    name_lower = {k.lower().strip(): v for k, v in DEFAULT_CATEGORY_DEPARTMENT_MAP.items()}

    # Build category list — restrict to in-use categories by default
    if only_in_use:
        in_use_ids = {
            r.category_id
            for r in db.query(SupplierCategorization.category_id)
            .filter(SupplierCategorization.organization_id == org_id)
            .distinct()
            .all()
        }
        all_categories = (
            db.query(InkoopCategory)
            .filter(InkoopCategory.id.in_(in_use_ids))
            .all()
        ) if in_use_ids else []
    else:
        all_categories = db.query(InkoopCategory).all()

    existing_mappings = {
        m.category_id: m
        for m in db.query(CategoryDepartment)
        .filter(CategoryDepartment.organization_id == org_id)
        .all()
    }

    matched_by_name = 0
    matched_by_group = 0
    added = 0
    updated = 0
    skipped = 0
    no_match = 0

    for cat in all_categories:
        cat_name_lower = (cat.inkooppakket or "").lower().strip()
        afd = name_lower.get(cat_name_lower)
        if afd:
            matched_by_name += 1
        else:
            afd = DEFAULT_GROUP_DEPARTMENT_MAP.get((cat.groep or "").strip())
            if afd:
                matched_by_group += 1
            else:
                no_match += 1
                continue

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
        "matched_by_name": matched_by_name,
        "matched_by_group": matched_by_group,
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "no_match": no_match,
        "total_categories": len(all_categories),
    }
