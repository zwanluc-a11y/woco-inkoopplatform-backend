"""API for managing departments (afdelingen) linked to inkoop categories."""
from __future__ import annotations

from collections import defaultdict
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.category import InkoopCategory
from app.models.category_department import CategoryDepartment
from app.models.contract import Contract, ContractSupplier
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
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


@router.get("/insights")
def department_insights(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Comprehensive insights per department: spend, suppliers, contracts, top categories, trends."""
    # Fetch all category→department mappings for this org
    cat_dept_rows = (
        db.query(CategoryDepartment.category_id, CategoryDepartment.afdeling)
        .filter(CategoryDepartment.organization_id == org_id)
        .all()
    )
    cat_to_afd: dict[int, str] = {r.category_id: r.afdeling for r in cat_dept_rows}

    if not cat_to_afd:
        return {
            "departments": [],
            "totals": {
                "total_spend": 0,
                "total_suppliers": 0,
                "total_contracts": 0,
                "total_categories_mapped": 0,
            },
            "all_years": [],
        }

    # Get all categorizations with their categories and percentages
    categorizations = (
        db.query(SupplierCategorization)
        .filter(SupplierCategorization.organization_id == org_id)
        .all()
    )

    # Build supplier_id → list of (afdeling, percentage, category_id)
    supplier_dept_weights: dict[int, list[tuple[str, float, int]]] = defaultdict(list)
    for sc in categorizations:
        afd = cat_to_afd.get(sc.category_id)
        if afd:
            supplier_dept_weights[sc.supplier_id].append((afd, sc.percentage / 100.0, sc.category_id))

    # Get yearly spend
    yearly_spends = (
        db.query(SupplierYearlySpend)
        .filter(SupplierYearlySpend.organization_id == org_id)
        .all()
    )

    # Get supplier names for top categories
    supplier_names = {
        s.id: s.name for s in db.query(Supplier).filter(Supplier.organization_id == org_id).all()
    }

    # Get category names
    category_names = {
        c.id: c.inkooppakket for c in db.query(InkoopCategory).all()
    }

    # Get contracts with their suppliers, and resolve which department each contract belongs to
    contracts = (
        db.query(Contract).filter(Contract.organization_id == org_id).all()
    )
    contract_supplier_links = (
        db.query(ContractSupplier.contract_id, ContractSupplier.supplier_id)
        .join(Contract, Contract.id == ContractSupplier.contract_id)
        .filter(Contract.organization_id == org_id)
        .all()
    )
    contract_to_suppliers: dict[int, list[int]] = defaultdict(list)
    for r in contract_supplier_links:
        contract_to_suppliers[r.contract_id].append(r.supplier_id)

    # Aggregate per-department metrics
    dept_data: dict[str, dict] = defaultdict(lambda: {
        "name": "",
        "category_ids": set(),
        "supplier_ids": set(),
        "contract_ids": set(),
        "total_spend": 0.0,
        "spend_per_year": defaultdict(float),
        "transactions_per_year": defaultdict(int),
        "category_spend": defaultdict(float),
        "category_supplier_count": defaultdict(set),
    })
    all_years: set[int] = set()

    # Distribute supplier spend across departments by categorization weights
    for ys in yearly_spends:
        weights = supplier_dept_weights.get(ys.supplier_id, [])
        if not weights:
            continue
        all_years.add(ys.year)
        # Aggregate weight per dept (a supplier may have multiple cats in same dept)
        per_dept: dict[str, float] = defaultdict(float)
        per_dept_cats: dict[str, list[int]] = defaultdict(list)
        for afd, pct, cat_id in weights:
            per_dept[afd] += pct
            per_dept_cats[afd].append(cat_id)
        for afd, weight in per_dept.items():
            d = dept_data[afd]
            d["name"] = afd
            d["supplier_ids"].add(ys.supplier_id)
            spend = float(ys.total_amount) * weight
            d["total_spend"] += spend
            d["spend_per_year"][ys.year] += spend
            d["transactions_per_year"][ys.year] += int((ys.transaction_count or 0) * weight)
            for cat_id in per_dept_cats[afd]:
                d["category_ids"].add(cat_id)
                # Each category in this dept gets its share weighted by ITS own pct
                # Re-look up the specific pct for this supplier+category
                for sc_afd, sc_pct, sc_cat in weights:
                    if sc_cat == cat_id and sc_afd == afd:
                        d["category_spend"][cat_id] += float(ys.total_amount) * sc_pct
                        d["category_supplier_count"][cat_id].add(ys.supplier_id)
                        break

    # Allocate contracts to departments based on their suppliers' categorizations
    for c in contracts:
        sup_ids = contract_to_suppliers.get(c.id, [])
        depts_for_contract: set[str] = set()
        for sid in sup_ids:
            for afd, _, _ in supplier_dept_weights.get(sid, []):
                depts_for_contract.add(afd)
        for afd in depts_for_contract:
            dept_data[afd]["contract_ids"].add(c.id)

    # Build response
    departments_out = []
    for afd, d in dept_data.items():
        # Top categories
        top_cats = sorted(
            d["category_spend"].items(), key=lambda x: abs(x[1]), reverse=True
        )[:5]
        top_cats_out = [
            {
                "category_id": cid,
                "category_name": category_names.get(cid, "?"),
                "spend": round(spend, 2),
                "supplier_count": len(d["category_supplier_count"][cid]),
            }
            for cid, spend in top_cats
        ]

        # Year trend
        years_sorted = sorted(d["spend_per_year"].keys())
        spend_per_year = [
            {
                "year": y,
                "spend": round(d["spend_per_year"][y], 2),
                "transactions": d["transactions_per_year"][y],
            }
            for y in years_sorted
        ]

        departments_out.append({
            "name": afd,
            "category_count": len(d["category_ids"]),
            "supplier_count": len(d["supplier_ids"]),
            "contract_count": len(d["contract_ids"]),
            "total_spend": round(d["total_spend"], 2),
            "spend_per_year": spend_per_year,
            "top_categories": top_cats_out,
        })

    departments_out.sort(key=lambda x: x["total_spend"], reverse=True)

    # Totals
    total_spend = sum(d["total_spend"] for d in departments_out)
    total_suppliers_unique = len(set().union(*[dept_data[d]["supplier_ids"] for d in dept_data])) if dept_data else 0
    total_contracts_unique = len(set().union(*[dept_data[d]["contract_ids"] for d in dept_data])) if dept_data else 0

    # Unmapped supplier spend (suppliers without any dept-mapped category)
    mapped_supplier_ids = set().union(*[dept_data[d]["supplier_ids"] for d in dept_data]) if dept_data else set()
    unmapped_spend = 0.0
    unmapped_suppliers: set[int] = set()
    for ys in yearly_spends:
        if ys.supplier_id not in mapped_supplier_ids:
            unmapped_spend += float(ys.total_amount)
            unmapped_suppliers.add(ys.supplier_id)

    return {
        "departments": departments_out,
        "totals": {
            "total_spend": round(total_spend, 2),
            "total_suppliers": total_suppliers_unique,
            "total_contracts": total_contracts_unique,
            "total_categories_mapped": len(cat_to_afd),
        },
        "unmapped": {
            "supplier_count": len(unmapped_suppliers),
            "total_spend": round(unmapped_spend, 2),
        },
        "all_years": sorted(all_years),
    }


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
