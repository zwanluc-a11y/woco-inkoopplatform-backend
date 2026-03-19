from datetime import datetime
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class Threshold(Base):
    __tablename__ = "thresholds"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    threshold_period: Mapped[str] = mapped_column(String(20))
    diensten_leveringen: Mapped[float] = mapped_column(Numeric(12, 2))
    werken: Mapped[float] = mapped_column(Numeric(12, 2))
    ict_diensten: Mapped[float] = mapped_column(Numeric(12, 2))
    advies_diensten: Mapped[float] = mapped_column(Numeric(12, 2))
    is_default: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
