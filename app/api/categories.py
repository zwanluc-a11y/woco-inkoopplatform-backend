from typing import Annotated, Optional
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from app.api.deps import get_current_user, get_current_user_or_token, get_db
from app.models.category import InkoopCategory
from app.models.user import User
from app.schemas.category import InkoopCategoryResponse
from app.services.export_service import ExportService

router = APIRouter(prefix="/categories", tags=["categories"])


@router.get("", response_model=list[InkoopCategoryResponse])
def list_categories(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    category_system: Optional[str] = Query("woco"),
):
    query = db.query(InkoopCategory).filter(InkoopCategory.category_system == category_system)
    return query.order_by(InkoopCategory.nummer).all()


@router.get("/grouped")
def list_categories_grouped(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    category_system: Optional[str] = Query("woco"),
):
    query = db.query(InkoopCategory).filter(InkoopCategory.category_system == category_system)
    categories = query.order_by(InkoopCategory.groep, InkoopCategory.nummer).all()
    grouped: dict = {}
    for cat in categories:
        if cat.groep not in grouped:
            grouped[cat.groep] = []
        grouped[cat.groep].append(InkoopCategoryResponse.model_validate(cat))
    return [{"groep": groep, "categories": cats} for groep, cats in grouped.items()]


@router.get("/export")
def export_categories(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user_or_token)],
    category_system: Optional[str] = Query("woco"),
):
    """Export the full category list as Excel."""
    service = ExportService(db)
    output = service.export_categories(category_system or "woco")
    filename = f"categorieenlijst_{category_system or 'woco'}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/search", response_model=list[InkoopCategoryResponse])
def search_categories(
    q: str = Query(..., min_length=2),
    category_system: Optional[str] = Query("woco"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    safe_q = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    search = f"%{safe_q}%"
    query = db.query(InkoopCategory).filter(
        InkoopCategory.category_system == category_system,
        InkoopCategory.inkooppakket.ilike(search, escape="\\")
        | InkoopCategory.definitie.ilike(search, escape="\\")
        | InkoopCategory.groep.ilike(search, escape="\\")
    )
    return query.order_by(InkoopCategory.nummer).all()
