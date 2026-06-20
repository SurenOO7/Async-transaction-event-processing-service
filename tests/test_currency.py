from decimal import Decimal
from unittest.mock import AsyncMock

import httpx

import pytest

from app.services.currency import CurrencyServiceError, to_usd


def deps(redis, *, base_url="https://fx.test", ttl=300):
    return dict(redis=redis, http=httpx.AsyncClient(base_url=base_url), base_url=base_url, ttl_seconds=ttl)


# AC3.3 — USD passes through: no cache read, no network.
async def test_usd_passthrough_no_lookup():
    redis = AsyncMock()
    result = await to_usd(Decimal("10.50"), "USD", **deps(redis))
    assert result == Decimal("10.50")
    redis.get.assert_not_called()


# AC2 — cache hit: rate served from Redis, no network fetch.
async def test_cache_hit_no_fetch(httpx_mock):
    redis = AsyncMock()
    redis.get.return_value = "0.5"
    result = await to_usd(Decimal("10"), "EUR", **deps(redis))
    assert result == Decimal("5.0")
    redis.get.assert_awaited_once_with("rate:EUR:USD")
    assert httpx_mock.get_requests() == []


# AC2 — cache miss: fetch from API, then write to cache with TTL.
async def test_cache_miss_fetches_and_caches(httpx_mock):
    redis = AsyncMock()
    redis.get.return_value = None
    httpx_mock.add_response(json={"rates": {"USD": 0.5}})
    result = await to_usd(Decimal("10"), "EUR", **deps(redis, ttl=300))
    assert result == Decimal("5.0")
    redis.set.assert_awaited_once()
    args, kwargs = redis.set.call_args
    assert args[0] == "rate:EUR:USD"
    assert Decimal(args[1]) == Decimal("0.5")
    assert kwargs.get("ex") == 300


# API failure surfaces as CurrencyServiceError (retryable -> retry/backoff -> DLQ).
async def test_api_failure_raises(httpx_mock):
    redis = AsyncMock()
    redis.get.return_value = None
    httpx_mock.add_response(status_code=503)
    with pytest.raises(CurrencyServiceError):
        await to_usd(Decimal("10"), "EUR", **deps(redis))


# Currency is normalized before the cache key is built.
async def test_currency_normalized_for_key():
    redis = AsyncMock()
    redis.get.return_value = "2"
    result = await to_usd(Decimal("3"), "eur", **deps(redis))
    assert result == Decimal("6")
    redis.get.assert_awaited_once_with("rate:EUR:USD")
