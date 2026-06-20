from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: Decimal = Field(..., gt=0)
    currency: str
    timestamp: datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def reject_numeric_timestamp(cls, v):
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


class TransactionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    amount: Decimal
    currency: str
    amount_usd: Decimal
    timestamp: datetime


class TransactionPage(BaseModel):
    items: list[TransactionRead]
    limit: int
    offset: int
