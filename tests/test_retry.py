from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.consumer.retry import backoff, encode_retry
from app.consumer.retry_worker import RetryWorker


class FakeRedis:
    """Minimal in-memory stand-in: a real-enough ZSET + append-only streams."""

    def __init__(self):
        self.zset: dict[str, float] = {}
        self.streams: dict[str, list] = {}

    async def zadd(self, key, mapping):
        self.zset.update(mapping)
        return len(mapping)

    async def zrangebyscore(self, key, min, max):
        return [m for m, s in sorted(self.zset.items(), key=lambda kv: kv[1]) if min <= s <= max]

    async def zrem(self, key, member):
        return 1 if self.zset.pop(member, None) is not None else 0

    async def zcard(self, key):
        return len(self.zset)

    async def xadd(self, key, fields):
        self.streams.setdefault(key, []).append(fields)
        return f"{len(self.streams[key])}-0"


def _fields(**overrides):
    fields = {
        "id": "evt-1",
        "user_id": "u1",
        "amount": "10.00",
        "currency": "EUR",
        "timestamp": "2026-06-19T12:00:00+00:00",
    }
    fields.update(overrides)
    return fields


def _session_factory():
    @asynccontextmanager
    async def factory():
        yield AsyncMock()

    return factory


def make_worker(redis, *, store, currency, max_attempts=5):
    return RetryWorker(
        redis=redis,
        session_factory=_session_factory(),
        currency=currency,
        store=store,
        zset="transactions:retry",
        dlq="transactions:dead",
        max_attempts=max_attempts,
    )


def _ok_currency():
    c = AsyncMock()
    c.to_usd.return_value = Decimal("11.00")
    return c


# AC4.3 — exponential backoff, capped.
def test_backoff_exponential_and_capped():
    assert backoff(1) == 1          # base
    assert backoff(2) == 2
    assert backoff(3) == 4
    assert backoff(4) == 8
    assert backoff(100) == 300      # capped at RETRY_MAX_DELAY_SECONDS
    # strictly non-decreasing
    seq = [backoff(n) for n in range(1, 12)]
    assert seq == sorted(seq)


# Only events whose due-time has passed are picked up; future ones stay.
async def test_only_due_events_picked_up():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(_fields(id="due"), 1): now - 1})
    await redis.zadd("transactions:retry", {encode_retry(_fields(id="future"), 1): now + 100})
    store = AsyncMock(return_value=True)
    worker = make_worker(redis, store=store, currency=_ok_currency())

    processed = await worker.run_once(now=now)

    assert processed == 1
    assert store.await_args.kwargs["id"] == "due"
    # the future event is untouched in the ZSET
    assert await redis.zcard("transactions:retry") == 1


# Success drains the event: removed from ZSET, nothing sent to DLQ.
async def test_success_removes_and_no_dlq():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(_fields(), 1): now - 1})
    store = AsyncMock(return_value=True)
    worker = make_worker(redis, store=store, currency=_ok_currency())

    await worker.run_once(now=now)

    assert await redis.zcard("transactions:retry") == 0
    assert redis.streams.get("transactions:dead") is None


# A repeated failure reschedules with a longer (backoff) delay and bumps attempt.
async def test_failure_reschedules_with_backoff():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(_fields(), 1): now - 1})
    store = AsyncMock(side_effect=RuntimeError("db down"))
    worker = make_worker(redis, store=store, currency=_ok_currency())

    await worker.run_once(now=now)

    assert await redis.zcard("transactions:retry") == 1
    member, score = next(iter(redis.zset.items()))
    # attempt 1 just failed => completed 2, scheduled backoff(2) ahead of now
    assert '"attempt": 2' in member
    assert score == pytest.approx(now + backoff(2))
    assert redis.streams.get("transactions:dead") is None


# AC4.5 — after MAX_ATTEMPTS the event lands in the dead-letter stream, not looping.
async def test_dlq_after_max_attempts():
    redis = FakeRedis()
    now = 1000.0
    # attempt=4 already completed; this failing run is attempt 5 => DLQ at max=5
    await redis.zadd("transactions:retry", {encode_retry(_fields(), 4): now - 1})
    store = AsyncMock(side_effect=RuntimeError("still down"))
    worker = make_worker(redis, store=store, currency=_ok_currency(), max_attempts=5)

    await worker.run_once(now=now)

    assert await redis.zcard("transactions:retry") == 0  # not rescheduled
    dead = redis.streams["transactions:dead"]
    assert len(dead) == 1
    assert dead[0]["id"] == "evt-1"
    assert dead[0]["attempts"] == "5"


# Empty queue is a no-op (main loop unaffected: retry worker is independent).
async def test_empty_queue_noop():
    redis = FakeRedis()
    worker = make_worker(redis, store=AsyncMock(), currency=_ok_currency())
    assert await worker.run_once(now=1000.0) == 0
