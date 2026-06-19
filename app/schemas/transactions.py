from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionEvent(BaseModel):
    # extra="forbid": unknown fields are a hard 422, not silently dropped.
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: Decimal = Field(..., gt=0)  # Decimal, not float — no money rounding error
    currency: str
    timestamp: datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def reject_numeric_timestamp(cls, v):
        # Contract is an ISO-8601 string; don't let Pydantic read a bare number as epoch.
        if isinstance(v, (bool, int, float)):
            raise ValueError("timestamp must be an ISO-8601 string")
        return v

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, v: str) -> str:
        if not (isinstance(v, str) and len(v) == 3 and v.isalpha()):
            raise ValueError("currency must be exactly 3 letters")
        return v.upper()


class TransactionAccepted(BaseModel):
    status: str = "accepted"
    id: str


class UserSummary(BaseModel):
    user_id: str
    total_usd: Decimal
    count: int
