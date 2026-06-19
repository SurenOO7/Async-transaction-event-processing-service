from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import transactions, users
from app.queue import create_consumer_group
from app.redis_client import create_redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = create_redis()
    await create_consumer_group(app.state.redis)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="event-service", lifespan=lifespan)
app.include_router(transactions.router)
app.include_router(users.router)
