from app.config import settings
from app.consumer.retry import schedule_retry
from app.schemas.transactions import TransactionEvent
from app.store import store_transaction


class ConsumerWorker:
    def __init__(
        self,
        *,
        redis,
        session_factory,
        currency,
        store=store_transaction,
        stream: str = settings.STREAM_KEY,
        group: str = settings.CONSUMER_GROUP,
        consumer: str = "worker-1",
        batch_size: int = 64,
        block_ms: int = 5000,
    ):
        self.redis = redis
        self.session_factory = session_factory
        self.currency = currency
        self.store = store
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.batch_size = batch_size
        self.block_ms = block_ms

    async def process_one(self, msg_id: str, fields: dict) -> None:
        try:
            event = TransactionEvent(**fields)  # re-validate; garbage -> retry -> DLQ
            amount_usd = await self.currency.to_usd(event.amount, event.currency)
            async with self.session_factory() as session:
                await self.store(
                    session,
                    id=event.id,
                    user_id=event.user_id,
                    amount=event.amount,
                    currency=event.currency,
                    amount_usd=amount_usd,
                    timestamp=event.timestamp,
                )
            # Ack ONLY after a successful store: a crash before this point leaves
            # the message in the PEL for redelivery (at-least-once), not lost.
            await self.redis.xack(self.stream, self.group, msg_id)
        except Exception:
            # Try once, then hand off to the retry queue and ack. Enqueue BEFORE
            # ack so the message is never gone from both places. Retries no
            # longer block this loop (the headline change vs. the reference).
            await schedule_retry(
                self.redis, fields, attempt=1, delay=settings.RETRY_BASE_DELAY_SECONDS
            )
            await self.redis.xack(self.stream, self.group, msg_id)

    async def run_once(self) -> int:
        resp = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: ">"},
            count=self.batch_size,
            block=self.block_ms,
        )
        if not resp:
            return 0
        processed = 0
        for _stream, messages in resp:
            for msg_id, fields in messages:
                await self.process_one(msg_id, fields)
                processed += 1
        return processed
