from __future__ import annotations
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ProcurementCalendarPhase(Base):
    __tablename__ = "procurement_calendar_phases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    calendar_item_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("procurement_calendar_items.id", ondelete="CASCADE")
    )
    phase_name: Mapped[str] = mapped_column(String(50))
    phase_order: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(
        String(20), default="niet_gestart"
    )  # niet_gestart, actief, afgerond, overgeslagen
    planned_start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    planned_end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
