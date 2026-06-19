from app.config import settings
from app.consumer.retry import schedule_retry
from app.schemas.transactions import TransactionEvent
from app.store import store_transaction


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
        event = TransactionEvent(**fields)  # re-validate; garbage -> retry -> DLQ
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
        # Ack ONLY after a successful store: a crash before this point leaves the
        # message in the PEL for redelivery (at-least-once), not lost.
        await redis.xack(stream, group, msg_id)
    except Exception:
        # Try once, then hand off to the retry queue and ack. Enqueue BEFORE ack
        # so the message is never gone from both places.
        await schedule_retry(redis, fields, attempt=1, delay=settings.RETRY_BASE_DELAY_SECONDS)
        await redis.xack(stream, group, msg_id)


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
                msg_id,
                fields,
                redis=redis,
                session_factory=session_factory,
                convert=convert,
                store=store,
                stream=stream,
                group=group,
            )
            processed += 1
    return processed
