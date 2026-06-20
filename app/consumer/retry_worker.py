import time

from app.config import settings
from app.consumer.retry import backoff, decode_retry, encode_retry
from app.consumer.worker import process_event
from app.services import metrics
from app.store import store_transaction


async def _handle_failure(redis, member, fields, attempt, exc, *, now, zset, dlq, max_attempts) -> None:
    completed = attempt + 1
    try:
        if completed >= max_attempts:
            await redis.xadd(dlq, {**fields, "attempts": str(completed), "error": str(exc)[:500]})
            metrics.record_dead_lettered()
        else:
            await redis.zadd(zset, {encode_retry(fields, completed): now + backoff(completed)})
    except Exception:
        await redis.zadd(zset, {member: now})


async def run_retry_once(
    *,
    redis,
    session_factory,
    convert,
    store=store_transaction,
    zset=settings.RETRY_ZSET_KEY,
    dlq=settings.DLQ_STREAM_KEY,
    max_attempts=settings.MAX_ATTEMPTS,
    batch_size=128,
    now=None,
) -> int:
    now = time.time() if now is None else now
    due = await redis.zrangebyscore(zset, min=0, max=now)
    processed = 0
    for member in due[:batch_size]:
        if not await redis.zrem(zset, member):
            continue
        fields, attempt = decode_retry(member)
        try:
            stored = await process_event(fields, convert=convert, session_factory=session_factory, store=store)
            metrics.record_processed("success" if stored else "duplicate")
        except Exception as exc:
            metrics.record_processed("failed")
            await _handle_failure(
                redis, member, fields, attempt, exc,
                now=now, zset=zset, dlq=dlq, max_attempts=max_attempts,
            )
        processed += 1
    metrics.set_retry_depth(await redis.zcard(zset))
    return processed
