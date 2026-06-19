from redis.exceptions import ResponseError

from app.config import settings
from app.schemas.transactions import TransactionEvent


def serialize(event: TransactionEvent) -> dict:
    # Stream fields are str->str; Decimal/datetime go in as lossless text.
    return {
        "id": event.id,
        "user_id": event.user_id,
        "amount": str(event.amount),
        "currency": event.currency,
        "timestamp": event.timestamp.isoformat(),
    }


async def push_event(redis, event: TransactionEvent) -> str:
    return await redis.xadd(settings.STREAM_KEY, serialize(event))


async def create_consumer_group(redis) -> None:
    # id="0" so the group reads from the start of the stream (never skips a
    # message that arrived before the group existed). mkstream creates the
    # stream if absent. BUSYGROUP just means it already exists — idempotent.
    try:
        await redis.xgroup_create(
            settings.STREAM_KEY, settings.CONSUMER_GROUP, id="0", mkstream=True
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
