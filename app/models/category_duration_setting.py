from sqlalchemy import Float, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class CategoryDurationSetting(Base):
    __tablename__ = "category_duration_settings"
    __table_args__ = (UniqueConstraint("organization_id", "category_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"))
    category_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("inkoop_categories.id")
    )
    expected_duration_years: Mapped[float] = mapped_column(
        Float, default=4.0
    )  # Default 4 years
