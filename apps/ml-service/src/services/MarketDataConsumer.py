import asyncio
import json
import logging
import os
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from src.services.FeatureEngineer import FeatureEngineer

class MarketDataConsumer:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = None
        self.pubsub = None
        self.channel = "market_data:ticker"
        self.running = False
        self.feature_engineer = FeatureEngineer()

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(self.channel)
        logger.info(f"Subscribed to {self.channel}")

    async def start(self):
        await self.connect()
        self.running = True
        logger.info("Market Data Consumer started")
        
        try:
            async for message in self.pubsub.listen():
                if not self.running:
                    break
                    
                if message["type"] == "message":
                    await self.process_message(message["data"])
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")

    async def process_message(self, data: str):
        try:
            ticker = json.loads(data)
            # logger.debug(f"Received ticker: {ticker}")
            
            features = self.feature_engineer.update(ticker['timestamp'], ticker['price'])
            if features:
                # Add symbol to features
                features['symbol'] = ticker['symbol']
                features['timestamp'] = ticker['timestamp']
                
                logger.info(f"Valid Signal: {ticker['symbol']} RSI={features['rsi_14']:.2f} SMA={features['sma_20']:.2f}")
                
                # Publish to Redis
                if self.redis:
                    await self.redis.publish("ml:features", json.dumps(features))
        except json.JSONDecodeError:
            logger.error("Failed to decode message")

    async def stop(self):
        self.running = False
        if self.pubsub:
            await self.pubsub.unsubscribe(self.channel)
        if self.redis:
            await self.redis.close()
        logger.info("Market Data Consumer stopped")
