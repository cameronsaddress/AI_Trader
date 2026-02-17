import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
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
        self.feature_engineers: dict[str, FeatureEngineer] = {}

    def _normalize_timestamp_ms(self, raw_timestamp):
        if isinstance(raw_timestamp, (int, float)) and not isinstance(raw_timestamp, bool):
            if math.isfinite(float(raw_timestamp)):
                return int(float(raw_timestamp))

        if isinstance(raw_timestamp, str):
            value = raw_timestamp.strip()
            if value:
                try:
                    parsed_num = float(value)
                    if math.isfinite(parsed_num):
                        return int(parsed_num)
                except ValueError:
                    pass

                try:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    pass

        return int(time.time() * 1000)

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

            symbol = ticker.get('symbol', 'UNKNOWN')
            timestamp_ms = self._normalize_timestamp_ms(ticker.get('timestamp'))
            price = ticker.get('price')
            if price is None:
                return

            if symbol not in self.feature_engineers:
                self.feature_engineers[symbol] = FeatureEngineer()
            features = self.feature_engineers[symbol].update(timestamp_ms, price)
            if features:
                # Add symbol to features
                features['symbol'] = symbol
                features['timestamp'] = timestamp_ms
                
                logger.info(f"Valid Signal: {symbol} RSI={features['rsi_14']:.2f} SMA={features['sma_20']:.2f}")
                
                # Sanitize NaN/Inf before JSON serialization (RSI can be NaN
                # when the rolling window has no losses).
                for k, v in list(features.items()):
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        features[k] = None

                # Publish to Redis
                if self.redis:
                    await self.redis.publish("ml:features", json.dumps(features))
        except json.JSONDecodeError:
            logger.error("Failed to decode message")
        except Exception as e:
            logger.error(f"Failed to process ticker message: {e}")

    async def stop(self):
        self.running = False
        if self.pubsub:
            await self.pubsub.unsubscribe(self.channel)
        if self.redis:
            await self.redis.close()
        logger.info("Market Data Consumer stopped")
