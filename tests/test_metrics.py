from decimal import Decimal
from unittest.mock import AsyncMock

from httpx import ASGITransport, AsyncClient
from prometheus_client import REGISTRY

from app.consumer.retry import encode_retry
from app.consumer.retry_worker import run_retry_once
from app.consumer.worker import process_one
from app.main import app
from app.services import metrics
from tests.helpers import FakeRedis, make_fields as _fields, session_factory as _sf


def _val(name, **labels):
    return REGISTRY.get_sample_value(name, labels or None) or 0.0


def test_record_processed_increments():
    before = _val("events_processed_total", status="success")
    metrics.record_processed("success")
    assert _val("events_processed_total", status="success") == before + 1


async def test_consumer_success_and_failure_metrics():
    before_s = _val("events_processed_total", status="success")
    await process_one(
        "1-0", _fields(), redis=AsyncMock(), session_factory=_sf(),
        convert=AsyncMock(return_value=Decimal("11")), store=AsyncMock(return_value=True),
        stream="s", group="g",
    )
    assert _val("events_processed_total", status="success") == before_s + 1

    before_f = _val("events_processed_total", status="failed")
    await process_one(
        "1-0", _fields(), redis=AsyncMock(), session_factory=_sf(),
        convert=AsyncMock(return_value=Decimal("11")), store=AsyncMock(side_effect=RuntimeError("x")),
        stream="s", group="g",
    )
    assert _val("events_processed_total", status="failed") == before_f + 1


async def test_dead_letter_metric():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("z", {encode_retry(_fields(), 4): now - 1})
    before = _val("events_dead_lettered_total")
    await run_retry_once(
        now=now, redis=redis, session_factory=_sf(),
        convert=AsyncMock(return_value=Decimal("11")), store=AsyncMock(side_effect=RuntimeError("x")),
        zset="z", dlq="d", max_attempts=5,
    )
    assert _val("events_dead_lettered_total") == before + 1


async def test_metrics_endpoint_scrapeable():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.get("/metrics")
    assert resp.status_code == 200
    assert "events_processed_total" in resp.text
