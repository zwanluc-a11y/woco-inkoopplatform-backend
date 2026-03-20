from __future__ import annotations

import logging
from datetime import date
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, get_db
from app.models.contract import Contract, ContractSupplier
from app.models.supplier import Supplier
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.transaction import Transaction
from app.models.user import User
from app.schemas.contract import ContractCreate, ContractResponse, ContractUpdate

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/organizations/{org_id}/contracts",
    tags=["contracts"],
)


def _contract_to_response(contract: Contract, db: Session | None = None) -> dict:
    """Convert a Contract ORM object to a response dict with category_name and supplier spend."""
    # Enrich suppliers with spend data
    supplier_list = []
    for s in (contract.suppliers or []):
        sup_data = {
            "id": s.id,
            "name": s.name,
            "normalized_name": s.normalized_name,
            "total_spend": 0.0,
        }
        if db:
            spend = (
                db.query(func.sum(SupplierYearlySpend.total_amount))
                .filter(SupplierYearlySpend.supplier_id == s.id)
                .scalar()
            )
            sup_data["total_spend"] = float(spend) if spend else 0.0
        supplier_list.append(sup_data)

    data = {
        "id": contract.id,
        "organization_id": contract.organization_id,
        "name": contract.name,
        "contract_number": contract.contract_number,
        "contract_type": contract.contract_type,
        "category_id": contract.category_id,
        "category_name": (
            contract.category.inkooppakket if contract.category else None
        ),
        "start_date": contract.start_date,
        "end_date": contract.end_date,
        "extension_options": contract.extension_options,
        "max_end_date": contract.max_end_date,
        "estimated_value": float(contract.estimated_value) if contract.estimated_value else None,
        "is_ingekocht_via_procedure": contract.is_ingekocht_via_procedure,
        "status": contract.status,
        "notes": contract.notes,
        "created_at": contract.created_at,
        "updated_at": contract.updated_at,
        "suppliers": supplier_list,
    }
    return data


@router.get("")
async def list_contracts(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    status: Optional[str] = None,
    search: Optional[str] = None,
):
    query = db.query(Contract).filter(Contract.organization_id == org_id)
    if status:
        query = query.filter(Contract.status == status)
    if search:
        safe_search = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        query = query.filter(Contract.name.ilike(f"%{safe_search}%", escape="\\"))
    contracts = query.order_by(Contract.end_date.asc().nullslast()).all()
    return [_contract_to_response(c, db) for c in contracts]


@router.post("")
async def create_contract(
    org_id: int,
    data: ContractCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    contract = Contract(
        organization_id=org_id,
        name=data.name,
        contract_number=data.contract_number,
        contract_type=data.contract_type,
        category_id=data.category_id,
        start_date=data.start_date,
        end_date=data.end_date,
        extension_options=data.extension_options,
        max_end_date=data.max_end_date,
        estimated_value=data.estimated_value,
        is_ingekocht_via_procedure=data.is_ingekocht_via_procedure,
        status=data.status,
        notes=data.notes,
    )

    # Auto-determine status based on dates
    today = date.today()
    if contract.end_date:
        if contract.end_date < today:
            contract.status = "expired"
        elif (contract.end_date - today).days <= 365:
            contract.status = "expiring"

    db.add(contract)
    db.flush()

    # Link suppliers
    if data.supplier_ids:
        for sid in data.supplier_ids:
            supplier = db.query(Supplier).filter(
                Supplier.id == sid, Supplier.organization_id == org_id
            ).first()
            if supplier:
                db.add(ContractSupplier(contract_id=contract.id, supplier_id=sid))

    db.commit()
    db.refresh(contract)
    return _contract_to_response(contract, db)


@router.get("/{contract_id}")
async def get_contract(
    org_id: int,
    contract_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    contract = (
        db.query(Contract)
        .filter(Contract.id == contract_id, Contract.organization_id == org_id)
        .first()
    )
    if not contract:
        raise HTTPException(status_code=404, detail="Contract niet gevonden")
    return _contract_to_response(contract, db)


@router.put("/{contract_id}")
async def update_contract(
    org_id: int,
    contract_id: int,
    data: ContractUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    contract = (
        db.query(Contract)
        .filter(Contract.id == contract_id, Contract.organization_id == org_id)
        .first()
    )
    if not contract:
        raise HTTPException(status_code=404, detail="Contract niet gevonden")

    update_data = data.model_dump(exclude_unset=True)
    supplier_ids = update_data.pop("supplier_ids", None)

    for key, val in update_data.items():
        setattr(contract, key, val)

    # Auto-determine status based on dates
    today = date.today()
    if contract.end_date:
        if contract.end_date < today:
            contract.status = "expired"
        elif (contract.end_date - today).days <= 365:
            contract.status = "expiring"

    if supplier_ids is not None:
        # Remove existing links
        db.query(ContractSupplier).filter(
            ContractSupplier.contract_id == contract_id
        ).delete()
        # Add new links (verify each supplier belongs to this org)
        for sid in supplier_ids:
            supplier = db.query(Supplier).filter(
                Supplier.id == sid, Supplier.organization_id == org_id
            ).first()
            if supplier:
                db.add(ContractSupplier(contract_id=contract_id, supplier_id=sid))

    db.commit()
    db.refresh(contract)
    return _contract_to_response(contract, db)


@router.delete("/{contract_id}")
async def delete_contract(
    org_id: int,
    contract_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    contract = (
        db.query(Contract)
        .filter(Contract.id == contract_id, Contract.organization_id == org_id)
        .first()
    )
    if not contract:
        raise HTTPException(status_code=404, detail="Contract niet gevonden")

    db.query(ContractSupplier).filter(
        ContractSupplier.contract_id == contract_id
    ).delete()
    db.delete(contract)
    db.commit()
    return {"detail": "Contract verwijderd"}


@router.get("/summary/stats")
async def contract_stats(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Get summary statistics for contracts."""
    today = date.today()
    contracts = db.query(Contract).filter(Contract.organization_id == org_id).all()

    total = len(contracts)
    active = sum(1 for c in contracts if c.status == "active")
    expiring = sum(
        1 for c in contracts
        if c.end_date and 0 < (c.end_date - today).days <= 365
    )
    expired = sum(
        1 for c in contracts
        if c.end_date and c.end_date < today
    )
    aanbesteed = sum(1 for c in contracts if c.is_ingekocht_via_procedure)

    return {
        "total": total,
        "active": active,
        "expiring": expiring,
        "expired": expired,
        "aanbesteed": aanbesteed,
    }


@router.post("/match-suppliers")
async def match_contract_suppliers(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Retroactively fuzzy-match contract suppliers to spend suppliers.

    For each supplier linked to a contract that has no transactions,
    try to find a matching spend supplier using fuzzy name matching.
    If found, re-link the contract to the spend supplier.
    """
    from rapidfuzz import fuzz, process as rfprocess
    from app.services.import_service import normalize_supplier_name

    # 1. Get all supplier IDs that have transactions (spend suppliers)
    spend_supplier_ids = set(
        sid for (sid,) in
        db.query(Transaction.supplier_id)
        .filter(Transaction.organization_id == org_id)
        .distinct()
        .all()
    )

    # 2. Load spend suppliers indexed by normalized_name
    spend_suppliers = (
        db.query(Supplier)
        .filter(Supplier.organization_id == org_id, Supplier.id.in_(spend_supplier_ids))
        .all()
    )
    spend_map: dict[str, Supplier] = {}
    for s in spend_suppliers:
        # Re-normalize with improved function to catch more matches
        norm = normalize_supplier_name(s.name)
        spend_map[norm] = s

    if not spend_map:
        return {"matched": 0, "results": []}

    # 3. Get all contracts with their supplier links
    contracts = (
        db.query(Contract)
        .filter(Contract.organization_id == org_id)
        .all()
    )

    results = []
    for contract in contracts:
        for contract_supplier in list(contract.suppliers or []):
            # Skip suppliers that already have transactions (already a spend supplier)
            if contract_supplier.id in spend_supplier_ids:
                continue

            # Try fuzzy match against spend suppliers
            norm = normalize_supplier_name(contract_supplier.name)

            # First try exact normalized match
            matched_spend = spend_map.get(norm)

            # If no exact match, try fuzzy
            if not matched_spend and spend_map:
                match = rfprocess.extractOne(
                    norm,
                    spend_map.keys(),
                    scorer=fuzz.ratio,
                    score_cutoff=85,
                )
                if match:
                    matched_name, score, _ = match
                    matched_spend = spend_map[matched_name]
                    logger.info(
                        "Fuzzy matched contract supplier '%s' → spend supplier '%s' (score=%.0f%%)",
                        contract_supplier.name, matched_spend.name, score,
                    )

            if matched_spend and matched_spend.id != contract_supplier.id:
                # Re-link: update ContractSupplier to point to spend supplier
                # Check if link to spend supplier already exists
                existing_link = (
                    db.query(ContractSupplier)
                    .filter(
                        ContractSupplier.contract_id == contract.id,
                        ContractSupplier.supplier_id == matched_spend.id,
                    )
                    .first()
                )
                if not existing_link:
                    # Remove old link
                    db.query(ContractSupplier).filter(
                        ContractSupplier.contract_id == contract.id,
                        ContractSupplier.supplier_id == contract_supplier.id,
                    ).delete(synchronize_session=False)
                    # Create new link to spend supplier
                    db.add(ContractSupplier(
                        contract_id=contract.id,
                        supplier_id=matched_spend.id,
                    ))
                    results.append({
                        "contract_id": contract.id,
                        "contract_name": contract.name,
                        "old_supplier": contract_supplier.name,
                        "new_supplier": matched_spend.name,
                        "new_supplier_id": matched_spend.id,
                    })

    if results:
        db.commit()

    return {"matched": len(results), "results": results}
