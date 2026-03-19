from __future__ import annotations
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional

from app.database import Base


class Contract(Base):
    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    name: Mapped[str] = mapped_column(String(500))
    contract_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    contract_type: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )  # raamcontract, huur_lease, onderhoud, eenmalig, overig
    start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    extension_options: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    max_end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    estimated_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)
    is_ingekocht_via_procedure: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(20), default="active")
    category_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("inkoop_categories.id"), nullable=True
    )
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    import_session_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("import_sessions.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    organization = relationship("Organization", back_populates="contracts")
    category = relationship("InkoopCategory", lazy="selectin")
    suppliers = relationship("Supplier", secondary="contract_suppliers", lazy="selectin")


class ContractSupplier(Base):
    __tablename__ = "contract_suppliers"

    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id"), primary_key=True)
    supplier_id: Mapped[int] = mapped_column(Integer, ForeignKey("suppliers.id"), primary_key=True)
