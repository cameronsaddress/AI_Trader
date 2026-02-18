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
from src.services.EnsembleSignalModel import EnsembleSignalModel

class MarketDataConsumer:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = None
        self.pubsub = None
        self.channel = "market_data:ticker"
        self.running = False
        self.feature_engineers: dict[str, FeatureEngineer] = {}
        self.signal_models: dict[str, EnsembleSignalModel] = {}
        self.exception_counts: dict[str, int] = {}
        self.last_exception_scope: str | None = None
        self.last_exception_message: str | None = None
        self.last_exception_ts_ms = 0
        self.last_exception_log_ts_ms = 0
        self.exception_log_cooldown_ms = 60_000
        cooldown_raw = os.getenv("ML_CONSUMER_EXCEPTION_LOG_COOLDOWN_MS")
        if cooldown_raw is not None:
            try:
                parsed_cooldown = int(float(cooldown_raw.strip()))
                self.exception_log_cooldown_ms = max(1_000, min(3_600_000, parsed_cooldown))
            except Exception as exc:
                logger.warning(
                    "[ML][telemetry] invalid env ML_CONSUMER_EXCEPTION_LOG_COOLDOWN_MS=%r err=%s",
                    cooldown_raw,
                    exc,
                )

    def _record_exception(
        self,
        scope: str,
        exc: Exception,
        context: dict[str, object] | None = None,
        now_ms: int | None = None,
    ) -> None:
        ts_ms = int(now_ms if now_ms is not None else (time.time() * 1000))
        count = self.exception_counts.get(scope, 0) + 1
        self.exception_counts[scope] = count
        self.last_exception_scope = scope
        self.last_exception_message = f"{type(exc).__name__}: {exc}"[:240]
        self.last_exception_ts_ms = ts_ms

        if (
            self.last_exception_log_ts_ms <= 0
            or (ts_ms - self.last_exception_log_ts_ms) >= self.exception_log_cooldown_ms
        ):
            self.last_exception_log_ts_ms = ts_ms
            context_json = json.dumps(context or {}, default=str, ensure_ascii=True)[:280]
            logger.warning(
                "[ML][telemetry] exception scope=%s count=%d msg=%s context=%s",
                scope,
                count,
                self.last_exception_message,
                context_json,
            )

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
                except ValueError as exc:
                    self._record_exception(
                        "normalize_timestamp.numeric_parse",
                        exc,
                        {"timestamp_preview": value[:64]},
                    )

                try:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except ValueError as exc:
                    self._record_exception(
                        "normalize_timestamp.iso_parse",
                        exc,
                        {"timestamp_preview": value[:64]},
                    )

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
            self._record_exception("consumer_loop", e)
            logger.error(f"Error in consumer loop: {e}")

    async def process_message(self, data: str):
        try:
            ticker = json.loads(data)
            # logger.debug(f"Received ticker: {ticker}")

            raw_symbol = ticker.get('symbol')
            symbol = str(raw_symbol).strip().upper() if isinstance(raw_symbol, str) else ""
            if not symbol:
                return
            timestamp_ms = self._normalize_timestamp_ms(ticker.get('timestamp'))
            raw_price = ticker.get('price')
            if raw_price is None:
                return
            try:
                price = float(raw_price)
            except (TypeError, ValueError) as exc:
                self._record_exception(
                    "process_message.price_parse",
                    exc,
                    {"symbol": symbol, "price_preview": str(raw_price)[:64]},
                    timestamp_ms,
                )
                return
            if not math.isfinite(price) or price <= 0:
                return

            if symbol not in self.feature_engineers:
                self.feature_engineers[symbol] = FeatureEngineer()
            if symbol not in self.signal_models:
                self.signal_models[symbol] = EnsembleSignalModel(symbol=symbol)

            features = self.feature_engineers[symbol].update(timestamp_ms, price)
            if features:
                model_signal = self.signal_models[symbol].update(timestamp_ms, features)
                features.update(model_signal)

                # Add symbol to features
                features['symbol'] = symbol
                features['timestamp'] = timestamp_ms

                rsi_log = features.get("rsi_14")
                sma_log = features.get("sma_20")
                ml_up_log = features.get("ml_prob_up")
                ml_conf_log = features.get("ml_signal_confidence")
                if not isinstance(rsi_log, (int, float)) or not math.isfinite(float(rsi_log)):
                    rsi_log = 0.0
                if not isinstance(sma_log, (int, float)) or not math.isfinite(float(sma_log)):
                    sma_log = 0.0
                if not isinstance(ml_up_log, (int, float)) or not math.isfinite(float(ml_up_log)):
                    ml_up_log = 0.5
                if not isinstance(ml_conf_log, (int, float)) or not math.isfinite(float(ml_conf_log)):
                    ml_conf_log = 0.0

                logger.info(
                    "Valid Signal: %s RSI=%.2f SMA=%.2f ML_UP=%.3f CONF=%.3f MODEL=%s",
                    symbol,
                    float(rsi_log),
                    float(sma_log),
                    float(ml_up_log),
                    float(ml_conf_log),
                    features.get("ml_model", "UNKNOWN"),
                )
                
                # Sanitize NaN/Inf before JSON serialization (RSI can be NaN
                # when the rolling window has no losses).
                for k, v in list(features.items()):
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        features[k] = None

                # Publish to Redis
                if self.redis:
                    await self.redis.publish("ml:features", json.dumps(features))
        except json.JSONDecodeError as exc:
            self._record_exception("process_message.json_decode", exc)
            logger.error("Failed to decode message")
        except Exception as e:
            self._record_exception("process_message", e)
            logger.error(f"Failed to process ticker message: {e}")

    async def stop(self):
        self.running = False
        if self.pubsub:
            await self.pubsub.unsubscribe(self.channel)
        if self.redis:
            await self.redis.close()
        logger.info("Market Data Consumer stopped")
