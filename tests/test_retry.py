from unittest.mock import AsyncMock

import pytest

from app.consumer.retry import backoff, encode_retry
from app.consumer.retry_worker import run_retry_once
from tests.helpers import FakeRedis, make_fields, ok_convert, session_factory


def _kw(redis, *, store, convert, max_attempts=5):
    return dict(
        redis=redis,
        session_factory=session_factory(),
        convert=convert,
        store=store,
        zset="transactions:retry",
        dlq="transactions:dead",
        max_attempts=max_attempts,
    )


def test_backoff_exponential_and_capped():
    assert backoff(1) == 1
    assert backoff(2) == 2
    assert backoff(3) == 4
    assert backoff(4) == 8
    assert backoff(100) == 300
    seq = [backoff(n) for n in range(1, 12)]
    assert seq == sorted(seq)


async def test_only_due_events_picked_up():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(make_fields(id="due"), 1): now - 1})
    await redis.zadd("transactions:retry", {encode_retry(make_fields(id="future"), 1): now + 100})
    store = AsyncMock(return_value=True)

    processed = await run_retry_once(now=now, **_kw(redis, store=store, convert=ok_convert()))

    assert processed == 1
    assert store.await_args.kwargs["id"] == "due"
    assert await redis.zcard("transactions:retry") == 1


async def test_success_removes_and_no_dlq():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(make_fields(), 1): now - 1})
    store = AsyncMock(return_value=True)

    await run_retry_once(now=now, **_kw(redis, store=store, convert=ok_convert()))

    assert await redis.zcard("transactions:retry") == 0
    assert redis.streams.get("transactions:dead") is None


async def test_failure_reschedules_with_backoff():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(make_fields(), 1): now - 1})
    store = AsyncMock(side_effect=RuntimeError("db down"))

    await run_retry_once(now=now, **_kw(redis, store=store, convert=ok_convert()))

    assert await redis.zcard("transactions:retry") == 1
    member, score = next(iter(redis.zset.items()))
    assert '"attempt": 2' in member
    assert score == pytest.approx(now + backoff(2))
    assert redis.streams.get("transactions:dead") is None


async def test_dlq_after_max_attempts():
    redis = FakeRedis()
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(make_fields(), 4): now - 1})
    store = AsyncMock(side_effect=RuntimeError("still down"))

    await run_retry_once(now=now, **_kw(redis, store=store, convert=ok_convert(), max_attempts=5))

    assert await redis.zcard("transactions:retry") == 0
    dead = redis.streams["transactions:dead"]
    assert len(dead) == 1
    assert dead[0]["id"] == "evt-1"
    assert dead[0]["attempts"] == "5"


async def test_outcome_write_failure_restores_member():
    redis = FakeRedis()

    async def boom(*args, **kwargs):
        raise RuntimeError("redis blip on xadd")

    redis.xadd = boom
    now = 1000.0
    await redis.zadd("transactions:retry", {encode_retry(make_fields(), 4): now - 1})
    store = AsyncMock(side_effect=RuntimeError("db down"))

    await run_retry_once(now=now, **_kw(redis, store=store, convert=ok_convert(), max_attempts=5))

    assert await redis.zcard("transactions:retry") == 1


async def test_empty_queue_noop():
    redis = FakeRedis()
    assert await run_retry_once(now=1000.0, **_kw(redis, store=AsyncMock(), convert=ok_convert())) == 0
