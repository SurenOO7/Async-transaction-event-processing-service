import redis.asyncio as redis

from app.config import settings


def create_redis() -> redis.Redis:
    return redis.from_url(settings.REDIS_URL, decode_responses=True)
