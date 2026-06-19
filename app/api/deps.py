from fastapi import Request

from app.db import SessionLocal


def get_redis(request: Request):
    return request.app.state.redis


async def get_db():
    async with SessionLocal() as session:
        yield session
