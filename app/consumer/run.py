import asyncio
import os

import httpx

from app.config import settings
from app.consumer.worker import run_once, run_reclaim_once
from app.db import SessionLocal
from app.queue import create_consumer_group
from app.redis_client import create_redis
from app.services.currency import build_convert
from app.services.metrics import serve_metrics


async def main() -> None:
    redis = create_redis()
    await create_consumer_group(redis)
    http = httpx.AsyncClient()
    convert = build_convert(redis, http)
    serve_metrics(settings.METRICS_PORT)
    consumer = os.getenv("HOSTNAME", "worker-1")
    try:
        while True:
            if await run_once(redis=redis, session_factory=SessionLocal, convert=convert, consumer=consumer) == 0:
                await run_reclaim_once(redis=redis, session_factory=SessionLocal, convert=convert, consumer=consumer)
                await asyncio.sleep(0.1)
    finally:
        await http.aclose()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
