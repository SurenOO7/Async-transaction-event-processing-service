from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.transactions import Transaction


def bounded_limit(limit: int | None) -> int:
    return min(limit or settings.DEFAULT_PAGE_SIZE, settings.MAX_PAGE_SIZE)


async def get_user_summary(session: AsyncSession, user_id: str) -> dict:
    stmt = select(
        func.coalesce(func.sum(Transaction.amount_usd), 0),
        func.count(),
    ).where(Transaction.user_id == user_id)
    total, count = (await session.execute(stmt)).one()
    return {"user_id": user_id, "total_usd": Decimal(str(total)), "count": count}


async def list_user_transactions(
    session: AsyncSession,
    user_id: str,
    *,
    from_: datetime | None = None,
    to: datetime | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[Transaction]:
    stmt = select(Transaction).where(Transaction.user_id == user_id)
    if from_ is not None:
        stmt = stmt.where(Transaction.timestamp >= from_)
    if to is not None:
        stmt = stmt.where(Transaction.timestamp <= to)
    stmt = (
        stmt.order_by(Transaction.timestamp.desc(), Transaction.id.desc())
        .limit(bounded_limit(limit))
        .offset(offset)
    )
    return list((await session.execute(stmt)).scalars().all())
