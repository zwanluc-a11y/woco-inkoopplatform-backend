from sqlalchemy import ForeignKey, Integer, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base

class SupplierYearlySpend(Base):
    __tablename__ = "supplier_yearly_spend"
    __table_args__ = (UniqueConstraint("supplier_id", "year"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True)
    supplier_id: Mapped[int] = mapped_column(Integer, ForeignKey("suppliers.id"), index=True)
    year: Mapped[int] = mapped_column(Integer)
    total_amount: Mapped[float] = mapped_column(Numeric(14, 2))
    transaction_count: Mapped[int] = mapped_column(Integer, default=0)
    supplier = relationship("Supplier", back_populates="yearly_spends")
