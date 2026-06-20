import time

from app.config import settings
from app.consumer.retry import backoff, decode_retry, encode_retry
from app.schemas.transactions import TransactionEvent
from app.services import metrics
from app.store import store_transaction


async def _reprocess(
    member, now, *, redis, session_factory, convert, store, zset, dlq, max_attempts
) -> None:
    fields, attempt = decode_retry(member)
    try:
        event = TransactionEvent(**fields)
        amount_usd = await convert(event.amount, event.currency)
        async with session_factory() as session:
            await store(
                session,
                id=event.id,
                user_id=event.user_id,
                amount=event.amount,
                currency=event.currency,
                amount_usd=amount_usd,
                timestamp=event.timestamp,
            )
        # Success: already removed from the ZSET by the claim; done.
        metrics.record_processed("success")
    except Exception as exc:
        metrics.record_processed("failed")
        completed = attempt + 1
        if completed >= max_attempts:
            await redis.xadd(dlq, {**fields, "attempts": str(completed), "error": str(exc)[:500]})
            metrics.record_dead_lettered()
        else:
            await redis.zadd(zset, {encode_retry(fields, completed): now + backoff(completed)})


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
        # Claim by removing first: if zrem returns 0 another worker took it.
        if not await redis.zrem(zset, member):
            continue
        await _reprocess(
            member,
            now,
            redis=redis,
            session_factory=session_factory,
            convert=convert,
            store=store,
            zset=zset,
            dlq=dlq,
            max_attempts=max_attempts,
        )
        processed += 1
    metrics.set_retry_depth(await redis.zcard(zset))
    return processed
