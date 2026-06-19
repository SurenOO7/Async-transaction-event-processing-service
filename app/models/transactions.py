from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    amount_usd: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Composite index drives the read APIs (per-user, time-ordered) without a cache.
    __table_args__ = (Index("ix_transactions_user_id_timestamp", "user_id", "timestamp"),)
