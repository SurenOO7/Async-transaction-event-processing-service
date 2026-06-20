import asyncio

import httpx

from app.config import settings
from app.consumer.retry_worker import run_retry_once
from app.db import SessionLocal
from app.redis_client import create_redis
from app.services.currency import build_convert
from app.services.metrics import serve_metrics


async def main() -> None:
    redis = create_redis()
    http = httpx.AsyncClient()
    convert = build_convert(redis, http)
    serve_metrics(settings.METRICS_PORT)
    try:
        while True:
            await run_retry_once(redis=redis, session_factory=SessionLocal, convert=convert)
            await asyncio.sleep(1)
    finally:
        await http.aclose()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
