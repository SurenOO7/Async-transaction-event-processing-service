from app.config import settings
from app.consumer.retry import schedule_retry
from app.schemas.transactions import TransactionEvent
from app.services import metrics
from app.store import store_transaction


async def process_event(fields, *, convert, session_factory, store) -> bool:
    """Validate -> convert -> store one event. Returns False if it was a duplicate.

    Shared by the main consumer and the retry worker so the conversion + storage
    contract lives in one place.
    """
    event = TransactionEvent(**fields)
    amount_usd = await convert(event.amount, event.currency)
    async with session_factory() as session:
        return await store(
            session,
            id=event.id,
            user_id=event.user_id,
            amount=event.amount,
            currency=event.currency,
            amount_usd=amount_usd,
            timestamp=event.timestamp,
        )


async def process_one(
    msg_id,
    fields,
    *,
    redis,
    session_factory,
    convert,
    store=store_transaction,
    stream=settings.STREAM_KEY,
    group=settings.CONSUMER_GROUP,
) -> None:
    try:
        stored = await process_event(fields, convert=convert, session_factory=session_factory, store=store)
        await redis.xack(stream, group, msg_id)
        metrics.record_processed("success" if stored else "duplicate")
    except Exception:
        await schedule_retry(redis, fields, attempt=1, delay=settings.RETRY_BASE_DELAY_SECONDS)
        await redis.xack(stream, group, msg_id)
        metrics.record_processed("failed")


async def run_once(
    *,
    redis,
    session_factory,
    convert,
    store=store_transaction,
    stream=settings.STREAM_KEY,
    group=settings.CONSUMER_GROUP,
    consumer="worker-1",
    batch_size=64,
    block_ms=5000,
) -> int:
    resp = await redis.xreadgroup(group, consumer, {stream: ">"}, count=batch_size, block=block_ms)
    if not resp:
        return 0
    processed = 0
    for _stream, messages in resp:
        for msg_id, fields in messages:
            await process_one(
                msg_id, fields,
                redis=redis, session_factory=session_factory, convert=convert,
                store=store, stream=stream, group=group,
            )
            processed += 1
    return processed


async def run_reclaim_once(
    *,
    redis,
    session_factory,
    convert,
    store=store_transaction,
    stream=settings.STREAM_KEY,
    group=settings.CONSUMER_GROUP,
    consumer="worker-1",
    min_idle_ms=60000,
    batch_size=64,
) -> int:
    result = await redis.xautoclaim(stream, group, consumer, min_idle_ms, start_id="0-0", count=batch_size)
    messages = result[1] if len(result) >= 2 else []
    processed = 0
    for msg_id, fields in messages:
        if not fields:
            await redis.xack(stream, group, msg_id)
            continue
        await process_one(
            msg_id, fields,
            redis=redis, session_factory=session_factory, convert=convert,
            store=store, stream=stream, group=group,
        )
        processed += 1
    return processed
