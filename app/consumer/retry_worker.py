import time

from app.config import settings
from app.consumer.retry import backoff, decode_retry, encode_retry
from app.schemas.transactions import TransactionEvent
from app.store import store_transaction


class RetryWorker:
    """Drains the retry ZSET out-of-band from the main consumer.

    Members are scored by due-time; we pop only those due now, reprocess once,
    and either succeed, reschedule with backoff, or dead-letter after the cap.
    Runs as its own process so a backlog of retries never slows ingestion.
    """

    def __init__(
        self,
        *,
        redis,
        session_factory,
        currency,
        store=store_transaction,
        zset: str = settings.RETRY_ZSET_KEY,
        dlq: str = settings.DLQ_STREAM_KEY,
        max_attempts: int = settings.MAX_ATTEMPTS,
        batch_size: int = 128,
    ):
        self.redis = redis
        self.session_factory = session_factory
        self.currency = currency
        self.store = store
        self.zset = zset
        self.dlq = dlq
        self.max_attempts = max_attempts
        self.batch_size = batch_size

    async def run_once(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        due = await self.redis.zrangebyscore(self.zset, min=0, max=now)
        processed = 0
        for member in due[: self.batch_size]:
            # Claim by removing first: if zrem returns 0 another worker took it.
            if not await self.redis.zrem(self.zset, member):
                continue
            await self._reprocess(member, now)
            processed += 1
        return processed

    async def _reprocess(self, member: str, now: float) -> None:
        fields, attempt = decode_retry(member)
        try:
            event = TransactionEvent(**fields)
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
            # Success: already removed from the ZSET by the claim; done.
        except Exception as exc:
            completed = attempt + 1
            if completed >= self.max_attempts:
                await self._dead_letter(fields, completed, exc)
            else:
                due_at = now + backoff(completed)
                await self.redis.zadd(self.zset, {encode_retry(fields, completed): due_at})

    async def _dead_letter(self, fields: dict, attempts: int, exc: Exception) -> None:
        await self.redis.xadd(
            self.dlq,
            {**fields, "attempts": str(attempts), "error": str(exc)[:500]},
        )
