from redis.exceptions import ResponseError

from app.config import settings
from app.schemas.transactions import TransactionEvent


def serialize(event: TransactionEvent) -> dict:
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
    try:
        await redis.xgroup_create(
            settings.STREAM_KEY, settings.CONSUMER_GROUP, id="0", mkstream=True
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
