from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.deps import get_db
from app.config import settings
from app.db import Base
from app.main import app
from app.models.transactions import Transaction
from app.services.users import get_user_summary, list_user_transactions

BASE = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine(
        "sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as s:
        yield s
    await engine.dispose()


async def _seed(session, user_id, n, *, amount_usd=Decimal("10"), start=BASE):
    for i in range(n):
        session.add(
            Transaction(
                id=f"{user_id}-{i}",
                user_id=user_id,
                amount=Decimal("10"),
                currency="USD",
                amount_usd=amount_usd,
                timestamp=start + timedelta(minutes=i),
            )
        )
    await session.commit()


async def test_summary_totals(session):
    await _seed(session, "u1", 3, amount_usd=Decimal("5"))
    await _seed(session, "u2", 2, amount_usd=Decimal("99"))
    summary = await get_user_summary(session, "u1")
    assert summary["count"] == 3
    assert summary["total_usd"] == Decimal("15")


async def test_summary_empty_user_zeros(session):
    summary = await get_user_summary(session, "nobody")
    assert summary["count"] == 0
    assert summary["total_usd"] == Decimal("0")


async def test_list_orders_newest_first(session):
    await _seed(session, "u1", 3)
    rows = await list_user_transactions(session, "u1")
    times = [r.timestamp for r in rows]
    assert times == sorted(times, reverse=True)


async def test_list_filters_by_time_range(session):
    await _seed(session, "u1", 5)
    rows = await list_user_transactions(
        session, "u1", from_=BASE + timedelta(minutes=1), to=BASE + timedelta(minutes=3)
    )
    assert {r.id for r in rows} == {"u1-1", "u1-2", "u1-3"}


async def test_list_paginates(session):
    await _seed(session, "u1", 5)
    page1 = await list_user_transactions(session, "u1", limit=2, offset=0)
    page2 = await list_user_transactions(session, "u1", limit=2, offset=2)
    assert [r.id for r in page1] == ["u1-4", "u1-3"]
    assert [r.id for r in page2] == ["u1-2", "u1-1"]


async def test_list_caps_page_size(session):
    await _seed(session, "u1", settings.MAX_PAGE_SIZE + 5)
    rows = await list_user_transactions(session, "u1", limit=settings.MAX_PAGE_SIZE + 1000)
    assert len(rows) == settings.MAX_PAGE_SIZE


async def test_list_no_range_returns_default_page(session):
    await _seed(session, "u1", settings.DEFAULT_PAGE_SIZE + 10)
    rows = await list_user_transactions(session, "u1")
    assert len(rows) == settings.DEFAULT_PAGE_SIZE


@pytest_asyncio.fixture
async def client(session):
    async def override():
        yield session

    app.dependency_overrides[get_db] = override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


async def test_http_summary(client, session):
    await _seed(session, "u1", 2, amount_usd=Decimal("7"))
    resp = await client.get("/users/u1/summary")
    assert resp.status_code == 200
    assert resp.json() == {"user_id": "u1", "total_usd": "14.00000000", "count": 2}


async def test_http_summary_empty_user_not_404(client):
    resp = await client.get("/users/ghost/summary")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert Decimal(body["total_usd"]) == Decimal("0")


async def test_http_transactions_filtered(client, session):
    await _seed(session, "u1", 5)
    resp = await client.get(
        "/users/u1/transactions",
        params={"from": (BASE + timedelta(minutes=2)).isoformat(), "limit": 10},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["limit"] == 10
    assert [t["id"] for t in body["items"]] == ["u1-4", "u1-3", "u1-2"]
