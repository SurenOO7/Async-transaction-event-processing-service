from decimal import Decimal
from functools import partial

import httpx

from app.config import settings


class CurrencyServiceError(Exception):
    """Raised when a USD rate can't be obtained; the consumer retries instead of dropping."""


def _cache_key(currency: str) -> str:
    return f"rate:{currency}:USD"


async def _fetch_rate(currency: str, *, http: httpx.AsyncClient, base_url: str) -> Decimal:
    try:
        resp = await http.get(
            f"{base_url.rstrip('/')}/latest",
            params={"base": currency, "symbols": "USD"},
        )
        resp.raise_for_status()
        data = resp.json()
        return Decimal(str(data["rates"]["USD"]))  # str() avoids float->Decimal error
    except (httpx.HTTPError, KeyError, ValueError, TypeError) as exc:
        raise CurrencyServiceError(f"could not fetch {currency}->USD rate") from exc


async def _get_rate(currency: str, *, redis, http: httpx.AsyncClient, base_url: str, ttl_seconds: int) -> Decimal:
    key = _cache_key(currency)
    cached = await redis.get(key)
    if cached is not None:
        return Decimal(cached)
    rate = await _fetch_rate(currency, http=http, base_url=base_url)
    await redis.set(key, str(rate), ex=ttl_seconds)  # TTL bounds staleness
    return rate


async def to_usd(amount: Decimal, currency: str, *, redis, http, base_url, ttl_seconds) -> Decimal:
    currency = currency.upper()
    if currency == "USD":
        return amount
    rate = await _get_rate(currency, redis=redis, http=http, base_url=base_url, ttl_seconds=ttl_seconds)
    return amount * rate


def build_convert(redis, http: httpx.AsyncClient):
    # Pre-bind the static FX deps so the hot loop just calls convert(amount, currency).
    return partial(
        to_usd,
        redis=redis,
        http=http,
        base_url=settings.FX_API_BASE_URL,
        ttl_seconds=settings.FX_RATE_TTL_SECONDS,
    )
