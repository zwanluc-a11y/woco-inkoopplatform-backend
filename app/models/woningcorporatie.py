from __future__ import annotations

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class WoningCorporatie(Base):
    """Seed table with 259 Dutch housing corporations."""

    __tablename__ = "woningcorporaties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    l_nummer: Mapped[str] = mapped_column(String(20), unique=True)
    naam: Mapped[str] = mapped_column(String(500))
    provincie: Mapped[str] = mapped_column(String(100), default="")
    grootte_klasse: Mapped[str] = mapped_column(String(10), default="")  # XS, S, M, L, XL
    aantal_vhe: Mapped[str] = mapped_column(String(100), default="")
