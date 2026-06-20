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


async def test_store_inserts_row(session):
    inserted = await store_transaction(session, **_record())
    assert inserted is True
    assert await _count(session) == 1
    row = await session.get(Transaction, "t1")
    assert row.amount == Decimal("10")
    assert row.currency == "EUR"
    assert row.amount_usd == Decimal("11")


async def test_duplicate_id_one_row(session):
    assert await store_transaction(session, **_record()) is True
    assert await store_transaction(session, **_record(amount=Decimal("999"))) is False
    assert await _count(session) == 1
    row = await session.get(Transaction, "t1")
    assert row.amount == Decimal("10")


async def test_integrity_error_caught(session, monkeypatch):
    await store_transaction(session, **_record())

    async def fake_get(*args, **kwargs):
        return None

    monkeypatch.setattr(session, "get", fake_get)
    result = await store_transaction(session, **_record())
    assert result is False
    assert await _count(session) == 1
