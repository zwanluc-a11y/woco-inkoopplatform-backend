from __future__ import annotations
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional

from app.database import Base


class ProcurementCalendarItem(Base):
    __tablename__ = "procurement_calendar_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    risk_assessment_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("risk_assessments.id"), nullable=True
    )
    contract_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("contracts.id"), nullable=True
    )
    category_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("inkoop_categories.id"), nullable=True
    )
    title: Mapped[str] = mapped_column(String(500))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    priority: Mapped[str] = mapped_column(String(10), default="medium")
    target_start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    target_publish_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    estimated_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), default="planned"
    )  # planned, in_progress, completed, cancelled
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    category = relationship("InkoopCategory", lazy="selectin")
    contract = relationship("Contract", lazy="selectin")
    phases = relationship(
        "ProcurementCalendarPhase",
        lazy="selectin",
        order_by="ProcurementCalendarPhase.phase_order",
        cascade="all, delete-orphan",
    )
