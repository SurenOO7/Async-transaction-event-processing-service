import json
import time

from app.config import settings


def encode_retry(fields: dict, attempt: int) -> str:
    return json.dumps({"fields": fields, "attempt": attempt})


def decode_retry(member: str) -> tuple[dict, int]:
    data = json.loads(member)
    return data["fields"], data["attempt"]


async def schedule_retry(redis, fields: dict, attempt: int, *, delay: float) -> None:
    # ZSET scored by due-time: the retry worker (Slice 6) pops members whose
    # score <= now. Backoff lives in the delay passed by the caller.
    due_at = time.time() + delay
    await redis.zadd(settings.RETRY_ZSET_KEY, {encode_retry(fields, attempt): due_at})
