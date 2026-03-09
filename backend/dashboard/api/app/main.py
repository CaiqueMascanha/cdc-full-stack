import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routers.ws import router
from app.consumers.kafka_consumer import consume

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)
app.include_router(router)