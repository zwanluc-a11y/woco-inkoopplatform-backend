from __future__ import annotations
from datetime import datetime
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional
from app.database import Base

class SupplierCategorization(Base):
    __tablename__ = "supplier_categorizations"
    __table_args__ = (UniqueConstraint("supplier_id", "category_id", name="uq_supplier_categorizations_supplier_category"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    supplier_id: Mapped[int] = mapped_column(Integer, ForeignKey("suppliers.id"))
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("inkoop_categories.id"))
    percentage: Mapped[float] = mapped_column(Float, default=100.0)
    source: Mapped[str] = mapped_column(String(20))
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ai_reasoning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    categorized_by: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    supplier = relationship("Supplier", back_populates="categorizations")
    category = relationship("InkoopCategory", lazy="selectin")
