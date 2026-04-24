from __future__ import annotations
from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


class CategoryDepartment(Base):
    """Maps an inkoop category to a department (afdeling) per organization."""

    __tablename__ = "category_departments"
    __table_args__ = (
        UniqueConstraint("organization_id", "category_id", name="uq_catdept_org_cat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    category_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("inkoop_categories.id", ondelete="CASCADE"), index=True
    )
    afdeling: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    category = relationship("InkoopCategory", lazy="selectin")
