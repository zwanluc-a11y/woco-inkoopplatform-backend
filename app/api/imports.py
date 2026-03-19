import logging
import tempfile
import threading
from pathlib import Path
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from app.api.deps import get_current_user, get_db, verify_org_beheerder, verify_org_membership
from app.database import SessionLocal
from app.models.contract import Contract, ContractSupplier
from app.models.import_session import ImportSession
from app.models.supplier import Supplier
from app.models.supplier_categorization import SupplierCategorization
from app.models.supplier_yearly_spend import SupplierYearlySpend
from app.models.transaction import Transaction
from app.models.user import User
from app.schemas.import_schemas import (
    ImportConfirmRequest,
    ImportStatusResponse,
    ImportUploadResponse,
)
from app.services.import_service import ImportService

router = APIRouter(
    prefix="/organizations/{org_id}/import",
    tags=["import"],
    dependencies=[Depends(verify_org_membership)],
)


@router.post("/upload", dependencies=[Depends(verify_org_beheerder)], response_model=ImportUploadResponse)
async def upload_excel(
    org_id: int,
    file: Annotated[UploadFile, File(...)],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    import_type: Annotated[Optional[str], Query()] = None,
):
    if not file.filename or not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(status_code=400, detail="Alleen .xlsx, .xls of .csv bestanden zijn toegestaan")

    # Save uploaded file to temp location
    suffix = Path(file.filename).suffix
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    content = await file.read()

    # Limit file size to 50 MB
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Bestand is te groot. Maximum is 50 MB.")
    tmp.write(content)
    tmp.flush()
    tmp_path = tmp.name
    tmp.close()

    service = ImportService(db)
    try:
        result = service.analyze_file(
            file_path=tmp_path,
            file_name=file.filename,
            org_id=org_id,
            user_id=current_user.id,
            import_type=import_type,
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("File analysis failed for %s: %s", file.filename, e)
        raise HTTPException(
            status_code=400,
            detail=f"Bestand kon niet worden geanalyseerd: {type(e).__name__}: {str(e)}",
        )


def _run_import_background(session_id: int, column_mapping: dict, year: Optional[int], org_id: int):
    """Run import processing in a background thread with its own DB session."""
    bg_db = SessionLocal()
    try:
        session = bg_db.query(ImportSession).filter(ImportSession.id == session_id).first()
        if not session:
            logger.error("Background import: session %s not found", session_id)
            return

        service = ImportService(bg_db)
        service.process_import(
            import_session=session,
            column_mapping=column_mapping,
            year=year,
        )
        logger.info("Background import completed for session %s, rows: %s", session_id, session.row_count)
    except Exception as e:
        logger.exception("Background import failed for session %s: %s", session_id, e)
        try:
            bg_db.rollback()  # Clear any pending DB error state
            session = bg_db.query(ImportSession).filter(ImportSession.id == session_id).first()
            if session:
                session.status = "failed"
                session.error_log = str(e)[:2000]  # Truncate long errors
                bg_db.commit()
        except Exception:
            logger.exception("Failed to update session status after error")
    finally:
        bg_db.close()


@router.post("/confirm", dependencies=[Depends(verify_org_beheerder)], response_model=ImportStatusResponse)
async def confirm_import(
    org_id: int,
    data: ImportConfirmRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    session = (
        db.query(ImportSession)
        .filter(ImportSession.id == data.import_session_id, ImportSession.organization_id == org_id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Import sessie niet gevonden")

    # Mark as processing and return immediately
    session.status = "processing"
    session.progress_current = 0
    session.progress_total = 0
    session.column_mapping = data.column_mapping
    if data.year:
        session.year = data.year
    db.commit()
    db.refresh(session)

    # Start background processing
    thread = threading.Thread(
        target=_run_import_background,
        args=(session.id, data.column_mapping, data.year, org_id),
        daemon=True,
    )
    thread.start()

    return session


@router.get("/status/{session_id}", response_model=ImportStatusResponse)
async def import_status(
    org_id: int,
    session_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Poll import progress."""
    # Expire cached data so we get fresh values from the DB
    db.expire_all()
    session = (
        db.query(ImportSession)
        .filter(ImportSession.id == session_id, ImportSession.organization_id == org_id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Import sessie niet gevonden")
    return session


@router.get("/history", response_model=list[ImportStatusResponse])
async def import_history(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    return (
        db.query(ImportSession)
        .filter(ImportSession.organization_id == org_id)
        .order_by(ImportSession.created_at.desc())
        .all()
    )


@router.post("/reset-spend", dependencies=[Depends(verify_org_beheerder)])
async def reset_spend_data(
    org_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Delete ALL spend-related data for this organization.

    Removes: transactions, supplier_yearly_spend, supplier_categorizations,
    suppliers, and non-contract import sessions.
    Contracts are kept intact.
    """
    # 1. Delete categorizations
    cat_count = db.query(SupplierCategorization).filter(
        SupplierCategorization.organization_id == org_id
    ).delete(synchronize_session=False)

    # 2. Delete yearly spend
    spend_count = db.query(SupplierYearlySpend).filter(
        SupplierYearlySpend.organization_id == org_id
    ).delete(synchronize_session=False)

    # 3. Delete transactions
    tx_count = db.query(Transaction).filter(
        Transaction.organization_id == org_id
    ).delete(synchronize_session=False)

    # 4. Delete contract-supplier links for suppliers we're about to delete
    supplier_ids = [
        s.id for s in
        db.query(Supplier.id).filter(Supplier.organization_id == org_id).all()
    ]
    if supplier_ids:
        db.query(ContractSupplier).filter(
            ContractSupplier.supplier_id.in_(supplier_ids)
        ).delete(synchronize_session=False)

    # 5. Delete suppliers
    sup_count = db.query(Supplier).filter(
        Supplier.organization_id == org_id
    ).delete(synchronize_session=False)

    # 6. Delete non-contract import sessions
    db.query(ImportSession).filter(
        ImportSession.organization_id == org_id,
        ImportSession.file_type != "contract_register",
    ).delete(synchronize_session=False)

    db.commit()

    return {
        "deleted_suppliers": sup_count,
        "deleted_transactions": tx_count,
        "deleted_spend_records": spend_count,
        "deleted_categorizations": cat_count,
    }


@router.delete("/{import_id}", dependencies=[Depends(verify_org_beheerder)], status_code=204)
async def delete_import(
    org_id: int,
    import_id: int,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
):
    """Delete an import session and all transactions it created."""
    session = (
        db.query(ImportSession)
        .filter(ImportSession.id == import_id, ImportSession.organization_id == org_id)
        .first()
    )
    if not session:
        raise HTTPException(status_code=404, detail="Import sessie niet gevonden")

    if session.file_type == "contract_register":
        # Delete contracts and their supplier links for this import
        contract_ids = [
            c.id for c in
            db.query(Contract.id).filter(Contract.import_session_id == import_id).all()
        ]
        if contract_ids:
            db.query(ContractSupplier).filter(
                ContractSupplier.contract_id.in_(contract_ids)
            ).delete(synchronize_session=False)
            db.query(Contract).filter(
                Contract.import_session_id == import_id
            ).delete(synchronize_session=False)
    else:
        # Delete transactions linked to this import session
        db.query(Transaction).filter(Transaction.import_session_id == import_id).delete()

        # Delete suppliers that were only created by this import (no transactions left)
        from sqlalchemy import func
        suppliers_with_txns = (
            db.query(Transaction.supplier_id)
            .filter(Transaction.organization_id == org_id)
            .distinct()
            .subquery()
        )
        orphaned_suppliers = (
            db.query(Supplier.id)
            .filter(
                Supplier.organization_id == org_id,
                ~Supplier.id.in_(suppliers_with_txns),
            )
            .all()
        )
        orphaned_ids = [s.id for s in orphaned_suppliers]
        if orphaned_ids:
            db.query(SupplierCategorization).filter(SupplierCategorization.supplier_id.in_(orphaned_ids)).delete(synchronize_session=False)
            db.query(SupplierYearlySpend).filter(SupplierYearlySpend.supplier_id.in_(orphaned_ids)).delete(synchronize_session=False)
            db.query(Supplier).filter(Supplier.id.in_(orphaned_ids)).delete(synchronize_session=False)

    db.delete(session)
    db.commit()
