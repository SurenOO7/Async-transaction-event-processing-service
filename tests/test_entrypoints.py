from decimal import Decimal
from unittest.mock import AsyncMock

import httpx

from app.services.currency import build_convert


async def test_build_convert_usd_passthrough():
    redis = AsyncMock()
    convert = build_convert(redis, httpx.AsyncClient())
    assert await convert(Decimal("5"), "USD") == Decimal("5")
    redis.get.assert_not_called()


def test_entrypoints_import():
    import app.consumer.retry_run  # noqa: F401
    import app.consumer.run  # noqa: F401
