from datetime import datetime
from decimal import Decimal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transactions import Transaction


async def store_transaction(
    session: AsyncSession,
    *,
    id: str,
    user_id: str,
    amount: Decimal,
    currency: str,
    amount_usd: Decimal,
    timestamp: datetime,
) -> bool:
    """Insert one transaction. Returns True if stored, False if it's a duplicate.

    Two-layer dedup: the read-check is a cheap optimization; the PK constraint
    is the real guarantee. Under concurrent workers both read-checks can miss,
    so the IntegrityError catch is what actually keeps it to one row.
    """
    if await session.get(Transaction, id) is not None:
        return False

    session.add(
        Transaction(
            id=id,
            user_id=user_id,
            amount=amount,
            currency=currency,
            amount_usd=amount_usd,
            timestamp=timestamp,
        )
    )
    try:
        await session.commit()
        return True
    except IntegrityError:
        await session.rollback()
        return False
