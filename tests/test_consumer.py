from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from app.consumer.worker import process_one, run_once, run_reclaim_once
from app.services.currency import CurrencyServiceError
from tests.helpers import make_fields, ok_convert, session_factory


def _kw(redis, *, store, convert):
    return dict(
        redis=redis,
        session_factory=session_factory(),
        convert=convert,
        store=store,
        stream="transactions",
        group="processors",
    )


async def test_success_stores_then_acks():
    redis = AsyncMock()
    store = AsyncMock(return_value=True)
    await process_one("1-0", make_fields(), **_kw(redis, store=store, convert=ok_convert()))

    store.assert_awaited_once()
    assert store.await_args.kwargs["id"] == "evt-1"
    assert store.await_args.kwargs["amount_usd"] == Decimal("11.00")
    redis.xack.assert_awaited_once_with("transactions", "processors", "1-0")
    redis.zadd.assert_not_called()


@pytest.mark.parametrize(
    "boom",
    [
        ("store", RuntimeError("db down")),
        ("convert", CurrencyServiceError("fx")),
    ],
)
async def test_failure_routes_to_retry(boom):
    which, exc = boom
    redis = AsyncMock()
    store = AsyncMock(return_value=True)
    convert = ok_convert()
    if which == "store":
        store.side_effect = exc
    else:
        convert.side_effect = exc

    await process_one("1-0", make_fields(), **_kw(redis, store=store, convert=convert))

    redis.zadd.assert_awaited_once()
    redis.xack.assert_awaited_once()


async def test_retry_enqueued_before_ack():
    redis = AsyncMock()
    store = AsyncMock(side_effect=RuntimeError("db down"))

    order = Mock()
    order.attach_mock(redis.zadd, "zadd")
    order.attach_mock(redis.xack, "xack")

    await process_one("1-0", make_fields(), **_kw(redis, store=store, convert=ok_convert()))

    names = [c[0] for c in order.mock_calls]
    assert names.index("zadd") < names.index("xack")


async def test_failure_tries_once_no_inline_retry():
    redis = AsyncMock()
    store = AsyncMock(side_effect=RuntimeError("db down"))
    convert = ok_convert()

    await process_one("1-0", make_fields(), **_kw(redis, store=store, convert=convert))

    assert store.await_count == 1
    assert convert.await_count == 1


async def test_duplicate_acks_no_retry():
    redis = AsyncMock()
    store = AsyncMock(return_value=False)
    await process_one("1-0", make_fields(), **_kw(redis, store=store, convert=ok_convert()))
    redis.xack.assert_awaited_once()
    redis.zadd.assert_not_called()


async def test_run_once_reads_and_dispatches():
    redis = AsyncMock()
    redis.xreadgroup.return_value = [["transactions", [("1-0", make_fields())]]]
    store = AsyncMock(return_value=True)

    processed = await run_once(
        redis=redis, session_factory=session_factory(), convert=ok_convert(),
        store=store, stream="transactions", group="processors", consumer="worker-1",
    )

    assert processed == 1
    redis.xreadgroup.assert_awaited_once()
    redis.xack.assert_awaited_once_with("transactions", "processors", "1-0")


async def test_reclaim_recovers_pending():
    redis = AsyncMock()
    redis.xautoclaim.return_value = ["0-0", [("5-0", make_fields())], []]
    store = AsyncMock(return_value=True)

    processed = await run_reclaim_once(
        redis=redis, session_factory=session_factory(), convert=ok_convert(),
        store=store, stream="transactions", group="processors", consumer="worker-1",
    )

    assert processed == 1
    redis.xautoclaim.assert_awaited_once()
    store.assert_awaited_once()
    redis.xack.assert_awaited_once_with("transactions", "processors", "5-0")
