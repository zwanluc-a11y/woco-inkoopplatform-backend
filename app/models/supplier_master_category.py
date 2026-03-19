from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlalchemy import DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class SupplierMasterCategory(Base):
    __tablename__ = "supplier_master_categories"
    __table_args__ = (UniqueConstraint("normalized_name", "category_id", "category_system", name="uq_master_supplier_category_v2"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    normalized_name: Mapped[str] = mapped_column(String(500), index=True)
    category_system: Mapped[str] = mapped_column(String(20), default="aedes", server_default="aedes")
    display_name: Mapped[str] = mapped_column(String(500))
    category_id: Mapped[int] = mapped_column(Integer, index=True)
    category_nummer: Mapped[str] = mapped_column(String(20))
    category_name: Mapped[str] = mapped_column(String(500))
    usage_count: Mapped[int] = mapped_column(Integer, default=1)
    source: Mapped[str] = mapped_column(String(20), default="auto")
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
