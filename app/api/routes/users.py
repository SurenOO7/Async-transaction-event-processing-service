from datetime import datetime

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_db
from app.schemas.transactions import TransactionPage, UserSummary
from app.services.users import bounded_limit, get_user_summary, list_user_transactions

router = APIRouter()


@router.get("/users/{user_id}/summary", response_model=UserSummary)
async def user_summary(user_id: str, db=Depends(get_db)) -> UserSummary:
    return UserSummary(**await get_user_summary(db, user_id))


@router.get("/users/{user_id}/transactions", response_model=TransactionPage)
async def user_transactions(
    user_id: str,
    db=Depends(get_db),
    from_: datetime | None = Query(None, alias="from"),
    to: datetime | None = Query(None, alias="to"),
    limit: int | None = Query(None, ge=1),
    offset: int = Query(0, ge=0),
) -> TransactionPage:
    rows = await list_user_transactions(db, user_id, from_=from_, to=to, limit=limit, offset=offset)
    return TransactionPage(items=rows, limit=bounded_limit(limit), offset=offset)
