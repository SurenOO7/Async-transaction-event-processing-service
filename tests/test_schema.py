from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.schemas.transactions import TransactionEvent, TransactionAccepted, UserSummary


def _valid_payload(**overrides):
    payload = {
        "id": "evt-1",
        "user_id": "user-1",
        "amount": "10.50",
        "currency": "USD",
        "timestamp": "2026-06-19T12:00:00Z",
    }
    payload.update(overrides)
    return payload


def test_valid_payload_parses():
    event = TransactionEvent(**_valid_payload())
    assert event.id == "evt-1"
    assert event.user_id == "user-1"
    assert event.currency == "USD"
    assert isinstance(event.amount, Decimal)
    assert event.amount == Decimal("10.50")


def test_currency_uppercased():
    event = TransactionEvent(**_valid_payload(currency="eur"))
    assert event.currency == "EUR"


@pytest.mark.parametrize("bad_amount", ["0", "-1", "-0.00000001"])
def test_amount_must_be_positive(bad_amount):
    with pytest.raises(ValidationError):
        TransactionEvent(**_valid_payload(amount=bad_amount))


@pytest.mark.parametrize("bad_currency", ["US", "USDD", "U1D", "12", "$$$", ""])
def test_currency_must_be_three_letters(bad_currency):
    with pytest.raises(ValidationError):
        TransactionEvent(**_valid_payload(currency=bad_currency))


def test_missing_field_rejected():
    payload = _valid_payload()
    del payload["user_id"]
    with pytest.raises(ValidationError):
        TransactionEvent(**payload)


def test_extra_field_rejected():
    with pytest.raises(ValidationError):
        TransactionEvent(**_valid_payload(unexpected="x"))


@pytest.mark.parametrize("bad_ts", ["not-a-date", "2026-13-40", "", 12345.6])
def test_unparseable_timestamp_rejected(bad_ts):
    with pytest.raises(ValidationError):
        TransactionEvent(**_valid_payload(timestamp=bad_ts))


def test_response_models_shape():
    accepted = TransactionAccepted(id="evt-1")
    assert accepted.status == "accepted"
    assert accepted.id == "evt-1"

    summary = UserSummary(user_id="user-1", total_usd=Decimal("0"), count=0)
    assert summary.total_usd == Decimal("0")
    assert summary.count == 0
