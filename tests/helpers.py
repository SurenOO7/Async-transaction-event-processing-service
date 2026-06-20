from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock


def make_fields(**overrides):
    fields = {
        "id": "evt-1",
        "user_id": "u1",
        "amount": "10.00",
        "currency": "EUR",
        "timestamp": "2026-06-19T12:00:00+00:00",
    }
    fields.update(overrides)
    return fields


def session_factory():
    @asynccontextmanager
    async def factory():
        yield AsyncMock()

    return factory


def ok_convert():
    return AsyncMock(return_value=Decimal("11.00"))


class FakeRedis:
    """In-memory stand-in: a real-enough ZSET + append-only streams."""

    def __init__(self):
        self.zset: dict[str, float] = {}
        self.streams: dict[str, list] = {}

    async def zadd(self, key, mapping):
        self.zset.update(mapping)
        return len(mapping)

    async def zrangebyscore(self, key, min, max):
        return [m for m, s in sorted(self.zset.items(), key=lambda kv: kv[1]) if min <= s <= max]

    async def zrem(self, key, member):
        return 1 if self.zset.pop(member, None) is not None else 0

    async def zcard(self, key):
        return len(self.zset)

    async def xadd(self, key, fields):
        self.streams.setdefault(key, []).append(fields)
        return f"{len(self.streams[key])}-0"
