import json
import time

from app.config import settings


def backoff(attempt: int) -> float:
    delay = settings.RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
    return min(delay, settings.RETRY_MAX_DELAY_SECONDS)


def encode_retry(fields: dict, attempt: int) -> str:
    return json.dumps({"fields": fields, "attempt": attempt})


def decode_retry(member: str) -> tuple[dict, int]:
    data = json.loads(member)
    return data["fields"], data["attempt"]


async def schedule_retry(redis, fields: dict, attempt: int, *, delay: float) -> None:
    due_at = time.time() + delay
    await redis.zadd(settings.RETRY_ZSET_KEY, {encode_retry(fields, attempt): due_at})
