from __future__ import annotations
from datetime import datetime
from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional
from app.database import Base

class InkoopCategory(Base):
    __tablename__ = "inkoop_categories"
    __table_args__ = (UniqueConstraint("category_system", "nummer", name="uq_category_system_nummer"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    category_system: Mapped[str] = mapped_column(String(20), default="aedes", server_default="aedes")
    groep: Mapped[str] = mapped_column(String(255))
    sector: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    nummer: Mapped[str] = mapped_column(String(20))
    inkooppakket: Mapped[str] = mapped_column(String(500))
    definitie: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    soort_inkoop: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, default="")
    cpv_code: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    homogeen: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
