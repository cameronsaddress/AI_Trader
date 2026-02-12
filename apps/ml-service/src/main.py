from fastapi import FastAPI
from contextlib import asynccontextmanager
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
    asyncio.create_task(consumer.start())
    yield
    # Shutdown
    logger.info("ML Service shutting down...")
    await consumer.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ml-service"}

@app.get("/")
async def root():
    return {"message": "AI Trader ML Service v1.0"}
