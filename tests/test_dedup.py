from datetime import datetime, timezone
from decimal import Decimal

import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models.transactions import Transaction
from app.store import store_transaction


@pytest_asyncio.fixture
async def session():
    # In-memory sqlite shared across sessions via StaticPool (single connection).
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as s:
        yield s
    await engine.dispose()


def _record(**overrides):
    record = dict(
        id="t1",
        user_id="u1",
        amount=Decimal("10"),
        currency="EUR",
        amount_usd=Decimal("11"),
        timestamp=datetime(2026, 6, 19, 12, 0, tzinfo=timezone.utc),
    )
    record.update(overrides)
    return record


async def _count(session):
    return (await session.execute(select(func.count()).select_from(Transaction))).scalar_one()


# AC3.5 — both original amount/currency and computed amount_usd are stored.
async def test_store_inserts_row(session):
    inserted = await store_transaction(session, **_record())
    assert inserted is True
    assert await _count(session) == 1
    row = await session.get(Transaction, "t1")
    assert row.amount == Decimal("10")
    assert row.currency == "EUR"
    assert row.amount_usd == Decimal("11")


# AC3.1 — same id twice => exactly one row; the first write wins.
async def test_duplicate_id_one_row(session):
    assert await store_transaction(session, **_record()) is True
    assert await store_transaction(session, **_record(amount=Decimal("999"))) is False
    assert await _count(session) == 1
    row = await session.get(Transaction, "t1")
    assert row.amount == Decimal("10")


# AC3.2 — concurrent insert race: the read-check misses, the PK constraint fires,
# IntegrityError is caught and treated as a duplicate (no crash, still one row).
async def test_integrity_error_caught(session, monkeypatch):
    await store_transaction(session, **_record())

    async def fake_get(*args, **kwargs):
        return None  # simulate the other worker's read-check passing

    monkeypatch.setattr(session, "get", fake_get)
    result = await store_transaction(session, **_record())
    assert result is False
    assert await _count(session) == 1
