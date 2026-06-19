from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from app.consumer.worker import process_one, run_once
from app.services.currency import CurrencyServiceError


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


def _kw(redis, *, store, convert):
    return dict(
        redis=redis,
        session_factory=_session_factory(),
        convert=convert,
        store=store,
        stream="transactions",
        group="processors",
    )


def _ok_convert():
    return AsyncMock(return_value=Decimal("11.00"))


# AC4.4 — success: store first, then XACK (ack only after successful storage).
async def test_success_stores_then_acks():
    redis = AsyncMock()
    store = AsyncMock(return_value=True)
    await process_one("1-0", _fields(), **_kw(redis, store=store, convert=_ok_convert()))

    store.assert_awaited_once()
    assert store.await_args.kwargs["id"] == "evt-1"
    assert store.await_args.kwargs["amount_usd"] == Decimal("11.00")
    redis.xack.assert_awaited_once_with("transactions", "processors", "1-0")
    redis.zadd.assert_not_called()


# AC4.1 / AC4.2 — failure routes to the retry queue and is not dropped.
@pytest.mark.parametrize(
    "boom",
    [
        ("store", RuntimeError("db down")),        # AC4.1 DB unavailable
        ("convert", CurrencyServiceError("fx")),    # AC4.2 rate lookup unavailable
    ],
)
async def test_failure_routes_to_retry(boom):
    which, exc = boom
    redis = AsyncMock()
    store = AsyncMock(return_value=True)
    convert = _ok_convert()
    if which == "store":
        store.side_effect = exc
    else:
        convert.side_effect = exc

    await process_one("1-0", _fields(), **_kw(redis, store=store, convert=convert))

    redis.zadd.assert_awaited_once()  # enqueued to retry ZSET — not dropped
    redis.xack.assert_awaited_once()  # handed off, removed from PEL


# AC4 — on failure, retry is enqueued BEFORE the XACK (no loss window).
async def test_retry_enqueued_before_ack():
    redis = AsyncMock()
    store = AsyncMock(side_effect=RuntimeError("db down"))

    order = Mock()
    order.attach_mock(redis.zadd, "zadd")
    order.attach_mock(redis.xack, "xack")

    await process_one("1-0", _fields(), **_kw(redis, store=store, convert=_ok_convert()))

    names = [c[0] for c in order.mock_calls]
    assert names.index("zadd") < names.index("xack")


# "Try once" — a failure does not inline-retry (does not block the loop).
async def test_failure_tries_once_no_inline_retry():
    redis = AsyncMock()
    store = AsyncMock(side_effect=RuntimeError("db down"))
    convert = _ok_convert()

    await process_one("1-0", _fields(), **_kw(redis, store=store, convert=convert))

    assert store.await_count == 1
    assert convert.await_count == 1


# run_once reads a batch via XREADGROUP and dispatches each message.
async def test_run_once_reads_and_dispatches():
    redis = AsyncMock()
    redis.xreadgroup.return_value = [["transactions", [("1-0", _fields())]]]
    store = AsyncMock(return_value=True)

    processed = await run_once(
        redis=redis,
        session_factory=_session_factory(),
        convert=_ok_convert(),
        store=store,
        stream="transactions",
        group="processors",
        consumer="worker-1",
    )

    assert processed == 1
    redis.xreadgroup.assert_awaited_once()
    redis.xack.assert_awaited_once_with("transactions", "processors", "1-0")
