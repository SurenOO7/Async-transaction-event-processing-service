from fastapi import APIRouter, Depends

from app.api.deps import get_redis
from app.schemas.transactions import TransactionAccepted, TransactionEvent
from app.services.transactions import ingest_event

router = APIRouter()


@router.post("/transactions", status_code=202, response_model=TransactionAccepted)
async def ingest(event: TransactionEvent, redis=Depends(get_redis)) -> TransactionAccepted:
    await ingest_event(redis, event)
    return TransactionAccepted(id=event.id)
