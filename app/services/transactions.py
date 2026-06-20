from app.queue import push_event
from app.schemas.transactions import TransactionEvent


async def ingest_event(redis, event: TransactionEvent) -> str:
    # Enqueue only — no DB write on the hot path, so ingest stays fast under burst.
    return await push_event(redis, event)
