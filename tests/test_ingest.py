from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_redis
from app.main import app


def _payload(**overrides):
    payload = {
        "id": "evt-1",
        "user_id": "u1",
        "amount": "10.50",
        "currency": "eur",
        "timestamp": "2026-06-19T12:00:00Z",
    }
    payload.update(overrides)
    return payload


@pytest.fixture
def redis_mock():
    m = AsyncMock()
    m.xadd.return_value = "1-0"
    return m


@pytest_asyncio.fixture
async def client(redis_mock):
    app.dependency_overrides[get_redis] = lambda: redis_mock
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


# AC1.1 + AC2.1 — valid payload -> 202 and exactly one stream entry.
async def test_valid_returns_202_and_enqueues(client, redis_mock):
    resp = await client.post("/transactions", json=_payload())
    assert resp.status_code == 202
    body = resp.json()
    assert body == {"status": "accepted", "id": "evt-1"}

    redis_mock.xadd.assert_awaited_once()
    stream_key, fields = redis_mock.xadd.call_args.args
    assert stream_key == "transactions"
    assert fields["id"] == "evt-1"
    assert fields["currency"] == "EUR"   # AC1.3 normalized before enqueue
    assert fields["amount"] == "10.50"   # Decimal serialized losslessly as string


# AC1.2 — invalid payloads -> 422 and nothing enqueued.
@pytest.mark.parametrize("bad", [{"amount": "0"}, {"currency": "eu"}, {"currency": "EURO"}])
async def test_invalid_returns_422_no_enqueue(client, redis_mock, bad):
    resp = await client.post("/transactions", json=_payload(**bad))
    assert resp.status_code == 422
    redis_mock.xadd.assert_not_called()


async def test_missing_field_422_no_enqueue(client, redis_mock):
    payload = _payload()
    del payload["user_id"]
    resp = await client.post("/transactions", json=payload)
    assert resp.status_code == 422
    redis_mock.xadd.assert_not_called()
