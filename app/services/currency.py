from decimal import Decimal

import httpx


class CurrencyServiceError(Exception):
    """Raised when a USD rate can't be obtained; the consumer retries instead of dropping."""


class CurrencyService:
    def __init__(self, redis, http: httpx.AsyncClient, *, base_url: str, ttl_seconds: int):
        self.redis = redis
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _cache_key(currency: str) -> str:
        return f"rate:{currency}:USD"

    async def to_usd(self, amount: Decimal, currency: str) -> Decimal:
        currency = currency.upper()
        if currency == "USD":
            return amount
        return amount * await self._get_rate(currency)

    async def _get_rate(self, currency: str) -> Decimal:
        key = self._cache_key(currency)
        cached = await self.redis.get(key)
        if cached is not None:
            return Decimal(cached)

        rate = await self._fetch_rate(currency)
        await self.redis.set(key, str(rate), ex=self.ttl_seconds)  # TTL bounds staleness
        return rate

    async def _fetch_rate(self, currency: str) -> Decimal:
        try:
            resp = await self.http.get(
                f"{self.base_url}/latest",
                params={"base": currency, "symbols": "USD"},
            )
            resp.raise_for_status()
            data = resp.json()
            return Decimal(str(data["rates"]["USD"]))  # str() avoids float->Decimal error
        except (httpx.HTTPError, KeyError, ValueError, TypeError) as exc:
            raise CurrencyServiceError(f"could not fetch {currency}->USD rate") from exc
