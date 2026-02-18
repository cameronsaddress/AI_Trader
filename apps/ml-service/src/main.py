from fastapi import FastAPI
from contextlib import asynccontextmanager
from contextlib import suppress
import logging
from src.services.MarketDataConsumer import MarketDataConsumer
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = MarketDataConsumer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("ML Service starting up...")
    consumer_task = asyncio.create_task(consumer.start())
    yield
    # Shutdown
    logger.info("ML Service shutting down...")
    await consumer.stop()
    consumer_task.cancel()
    with suppress(asyncio.CancelledError):
        await consumer_task

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ml-service"}

@app.get("/")
async def root():
    return {"message": "AI Trader ML Service v1.0"}
