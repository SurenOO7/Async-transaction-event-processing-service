"""Pydantic v2 models for the transaction-event API.

`TransactionEvent` is the validation boundary for `POST /transactions`: anything
that doesn't parse here never reaches the queue. `extra="forbid"` makes unknown
fields a hard error so typos and payload smuggling fail loudly (422) instead of
being silently dropped.
"""
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionEvent(BaseModel):
    """The ingest payload: {id, user_id, amount, currency, timestamp}."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    # gt=0 enforces amount > 0. Decimal (not float) — money math must not carry
    # binary-float rounding error (AC3.4).
    amount: Decimal = Field(..., gt=0)
    # Exactly 3 ASCII letters, normalized to uppercase by the validator below.
    currency: str
    timestamp: datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def reject_numeric_timestamp(cls, v):
        # Our contract is an ISO-8601 string (or datetime). Pydantic would
        # otherwise treat a bare number as a Unix epoch — not part of the API.
        if isinstance(v, bool) or isinstance(v, (int, float)):
            raise ValueError("timestamp must be an ISO-8601 string")
        return v

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, v: str) -> str:
        if not (isinstance(v, str) and len(v) == 3 and v.isalpha()):
            raise ValueError("currency must be exactly 3 letters")
        return v.upper()


class TransactionAccepted(BaseModel):
    """202 response body for a successfully enqueued event."""

    status: str = "accepted"
    id: str


class UserSummary(BaseModel):
    """Response for GET /users/{user_id}/summary."""

    user_id: str
    total_usd: Decimal
    count: int
