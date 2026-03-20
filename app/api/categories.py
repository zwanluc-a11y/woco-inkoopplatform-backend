from typing import Annotated, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.api.deps import get_current_user, get_db
from app.models.category import InkoopCategory
from app.models.user import User
from app.schemas.category import InkoopCategoryResponse

router = APIRouter(prefix="/categories", tags=["categories"])


@router.get("", response_model=list[InkoopCategoryResponse])
async def list_categories(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    category_system: Optional[str] = Query("aedes"),
):
    query = db.query(InkoopCategory).filter(InkoopCategory.category_system == category_system)
    return query.order_by(InkoopCategory.nummer).all()


@router.get("/grouped")
async def list_categories_grouped(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    category_system: Optional[str] = Query("aedes"),
):
    query = db.query(InkoopCategory).filter(InkoopCategory.category_system == category_system)
    categories = query.order_by(InkoopCategory.groep, InkoopCategory.nummer).all()
    grouped: dict = {}
    for cat in categories:
        if cat.groep not in grouped:
            grouped[cat.groep] = []
        grouped[cat.groep].append(InkoopCategoryResponse.model_validate(cat))
    return [{"groep": groep, "categories": cats} for groep, cats in grouped.items()]


@router.get("/search", response_model=list[InkoopCategoryResponse])
async def search_categories(
    q: str = Query(..., min_length=2),
    category_system: Optional[str] = Query("aedes"),
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
