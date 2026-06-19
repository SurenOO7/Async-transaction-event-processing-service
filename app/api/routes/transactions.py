from fastapi import APIRouter, Depends

from app.api.deps import get_redis
from app.queue import push_event
from app.schemas.transactions import TransactionAccepted, TransactionEvent

router = APIRouter()


@router.post("/transactions", status_code=202, response_model=TransactionAccepted)
async def ingest(event: TransactionEvent, redis=Depends(get_redis)) -> TransactionAccepted:
    # Enqueue only — no DB write here, so ingest stays fast under burst (AC1.4).
    await push_event(redis, event)
    return TransactionAccepted(id=event.id)
