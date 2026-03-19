from __future__ import annotations
from datetime import datetime
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, deferred, mapped_column, relationship
from typing import Optional
from app.database import Base

class Organization(Base):
    __tablename__ = "organizations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    org_type: Mapped[str] = mapped_column(String(50))  # woningcorporatie_klein, _middel, _groot, overig
    category_system: Mapped[str] = mapped_column(String(20), default="aedes")  # aedes, bu_woco
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_by: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    brand_logo_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    brand_screenshot_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    brand_primary_color: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    brand_secondary_color: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    brand_accent_color: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    brand_logo_data: Mapped[Optional[str]] = deferred(mapped_column(Text, nullable=True))
    brand_screenshot_data: Mapped[Optional[str]] = deferred(mapped_column(Text, nullable=True))
    thresholds = relationship("Threshold", lazy="selectin")
    suppliers = relationship("Supplier", lazy="dynamic")
    contracts = relationship("Contract", back_populates="organization", lazy="dynamic")
