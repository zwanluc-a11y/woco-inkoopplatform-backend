from __future__ import annotations
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional

from app.database import Base


class RiskAssessment(Base):
    __tablename__ = "risk_assessments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    category_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("inkoop_categories.id")
    )
    assessment_year: Mapped[int] = mapped_column(Integer)
    yearly_spend: Mapped[float] = mapped_column(Numeric(14, 2))
    supplier_count: Mapped[int] = mapped_column(Integer)
    duration_years: Mapped[float] = mapped_column(Float, default=4.0)
    estimated_contract_value: Mapped[float] = mapped_column(Numeric(14, 2))
    internal_threshold: Mapped[float] = mapped_column(Numeric(14, 2))
    threshold_type: Mapped[str] = mapped_column(String(100))
    risk_level: Mapped[str] = mapped_column(
        String(30)
    )  # offertetraject, meervoudig_onderhands, enkelvoudig_onderhands, vrije_inkoop
    policy_compliant: Mapped[bool] = mapped_column(Boolean, default=True)
    has_contract: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    assessed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    assessed_by: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True
    )

    category = relationship("InkoopCategory", lazy="selectin")
