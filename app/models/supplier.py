from __future__ import annotations
from datetime import datetime
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional
from app.database import Base

class Supplier(Base):
    __tablename__ = "suppliers"
    __table_args__ = (UniqueConstraint("organization_id", "supplier_code"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    name: Mapped[str] = mapped_column(String(500))
    supplier_code: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    normalized_name: Mapped[str] = mapped_column(String(500), index=True)
    is_beinvloedbaar: Mapped[bool] = mapped_column(Boolean, default=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    transactions = relationship("Transaction", back_populates="supplier", lazy="dynamic")
    yearly_spends = relationship("SupplierYearlySpend", back_populates="supplier", lazy="selectin")
    categorizations = relationship("SupplierCategorization", back_populates="supplier", lazy="selectin")
