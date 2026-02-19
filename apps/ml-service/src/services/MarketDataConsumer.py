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

SCAN_STRATEGY_SYMBOL_MAP = {
    "BTC_5M": "BTC-USD",
    "BTC_15M": "BTC-USD",
    "CEX_SNIPER": "BTC-USD",
    "CEX_ARB": "BTC-USD",
    "OBI_SCALPER": "BTC-USD",
    "GRAPH_ARB": "BTC-USD",
    "ETH_5M": "ETH-USD",
    "ETH_15M": "ETH-USD",
    "SOL_5M": "SOL-USD",
    "SOL_15M": "SOL-USD",
}

class MarketDataConsumer:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = None
        self.pubsub = None
        self.ticker_channel = os.getenv("ML_TICKER_CHANNEL", "market_data:ticker")
        self.scan_channel = os.getenv("ML_SCAN_CHANNEL", "arbitrage:scan")
        self.microstructure_max_age_ms = int(float(os.getenv("ML_MICROSTRUCTURE_MAX_AGE_MS", "15000")))
        self.running = False
        self.feature_engineers: dict[str, FeatureEngineer] = {}
        self.signal_models: dict[str, EnsembleSignalModel] = {}
        self.microstructure_by_symbol: dict[str, dict[str, float]] = {}
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

    async def _close_connections(self):
        if self.pubsub:
            try:
                await self.pubsub.unsubscribe(self.ticker_channel, self.scan_channel)
            except Exception as exc:
                self._record_exception("redis_pubsub_unsubscribe", exc)
            try:
                await self.pubsub.close()
            except Exception as exc:
                self._record_exception("redis_pubsub_close", exc)
            self.pubsub = None
        if self.redis:
            try:
                await self.redis.close()
            except Exception as exc:
                self._record_exception("redis_close", exc)
            self.redis = None

    def _normalize_epoch_ms(self, raw_epoch: float) -> int:
        if not math.isfinite(raw_epoch):
            return int(time.time() * 1000)
        magnitude = abs(raw_epoch)
        # Accept seconds/ms/us/ns and normalize to milliseconds.
        if magnitude < 1e11:
            return int(raw_epoch * 1000.0)
        if magnitude > 1e16:
            return int(raw_epoch / 1_000_000.0)
        if magnitude > 1e14:
            return int(raw_epoch / 1_000.0)
        return int(raw_epoch)

    def _normalize_timestamp_ms(self, raw_timestamp):
        if isinstance(raw_timestamp, (int, float)) and not isinstance(raw_timestamp, bool):
            if math.isfinite(float(raw_timestamp)):
                return self._normalize_epoch_ms(float(raw_timestamp))

        if isinstance(raw_timestamp, str):
            value = raw_timestamp.strip()
            if value:
                parsed_num = None
                try:
                    parsed_num = float(value)
                except ValueError:
                    parsed_num = None
                if parsed_num is not None and math.isfinite(parsed_num):
                    return self._normalize_epoch_ms(parsed_num)

                try:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except ValueError as exc:
                    self._record_exception(
                        "normalize_timestamp.parse",
                        exc,
                        {"timestamp_preview": value[:64]},
                    )

        return int(time.time() * 1000)

    async def connect(self):
        await self._close_connections()
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(self.ticker_channel, self.scan_channel)
        logger.info("Subscribed to %s and %s", self.ticker_channel, self.scan_channel)

    async def start(self):
        self.running = True
        logger.info("Market Data Consumer started")
        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while self.running:
            try:
                await self.connect()
                reconnect_delay = 1.0
                async for message in self.pubsub.listen():
                    if not self.running:
                        break
                    if message.get("type") != "message":
                        continue

                    channel = str(message.get("channel") or "")
                    if channel == self.scan_channel:
                        await self.process_scan_message(str(message.get("data") or ""))
                    else:
                        await self.process_ticker_message(str(message.get("data") or ""))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._record_exception("consumer_loop", e)
                logger.error("Error in consumer loop: %s", e)
            finally:
                await self._close_connections()

            if not self.running:
                break

            logger.warning("ML consumer reconnecting in %.1fs", reconnect_delay)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2.0)

    def _safe_number(self, raw_value):
        if isinstance(raw_value, bool):
            return None
        try:
            parsed = float(raw_value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed):
            return None
        return parsed

    def _infer_symbol_from_scan(self, scan: dict) -> str | None:
        strategy = str(scan.get("strategy") or "").strip().upper()
        if strategy in SCAN_STRATEGY_SYMBOL_MAP:
            return SCAN_STRATEGY_SYMBOL_MAP[strategy]

        raw_symbol = str(scan.get("symbol") or "").strip().upper()
        text_hint = f"{raw_symbol} {strategy}"
        if "BTC" in text_hint:
            return "BTC-USD"
        if "ETH" in text_hint:
            return "ETH-USD"
        if "SOL" in text_hint:
            return "SOL-USD"
        return None

    def _extract_microstructure_features(self, scan: dict) -> dict[str, float]:
        meta = scan.get("meta")
        if not isinstance(meta, dict):
            meta = {}

        yes_spread = self._safe_number(meta.get("yes_spread"))
        no_spread = self._safe_number(meta.get("no_spread"))
        spread = self._safe_number(meta.get("spread"))
        if spread is None and yes_spread is not None and no_spread is not None:
            spread = (yes_spread + no_spread) / 2.0

        yes_book_age = self._safe_number(meta.get("yes_book_age_ms"))
        no_book_age = self._safe_number(meta.get("no_book_age_ms"))
        book_age_ms = self._safe_number(meta.get("book_age_ms"))
        if book_age_ms is None and yes_book_age is not None and no_book_age is not None:
            book_age_ms = (yes_book_age + no_book_age) / 2.0

        features: dict[str, float] = {}
        scalar_mappings = {
            "micro_edge_to_spread_ratio": meta.get("edge_to_spread_ratio"),
            "micro_parity_deviation": meta.get("parity_deviation"),
            "micro_obi": meta.get("obi"),
            "micro_buy_pressure": meta.get("buy_pressure"),
            "micro_uptick_ratio": meta.get("uptick_ratio"),
            "micro_cluster_confidence": meta.get("cluster_confidence"),
            "micro_flow_acceleration": meta.get("flow_acceleration"),
            "micro_dynamic_cost_rate": meta.get("dynamic_cost_rate"),
            "micro_momentum_sigma": meta.get("momentum_sigma"),
        }

        for feature_name, raw_value in scalar_mappings.items():
            parsed = self._safe_number(raw_value)
            if parsed is not None:
                features[feature_name] = parsed

        if spread is not None:
            features["micro_spread"] = spread
        if book_age_ms is not None:
            features["micro_book_age_ms"] = max(0.0, book_age_ms)
        return features

    def _current_microstructure_features(self, symbol: str, now_ms: int) -> dict[str, float]:
        neutral = {
            "micro_spread": 0.0,
            "micro_book_age_ms": float(self.microstructure_max_age_ms),
            "micro_edge_to_spread_ratio": 0.0,
            "micro_parity_deviation": 0.0,
            "micro_obi": 0.0,
            "micro_buy_pressure": 0.5,
            "micro_uptick_ratio": 0.5,
            "micro_cluster_confidence": 0.0,
            "micro_flow_acceleration": 0.0,
            "micro_dynamic_cost_rate": 0.0,
            "micro_momentum_sigma": 0.0,
            "micro_data_fresh": 0.0,
            "micro_data_age_ms": -1.0,
        }

        snapshot = self.microstructure_by_symbol.get(symbol)
        if not snapshot:
            return neutral

        ts_ms = self._safe_number(snapshot.get("micro_ts_ms")) or 0.0
        age_ms = float(max(0, int(now_ms - ts_ms)))
        if age_ms > float(self.microstructure_max_age_ms):
            neutral["micro_data_age_ms"] = age_ms
            return neutral

        merged = dict(neutral)
        merged.update({k: v for k, v in snapshot.items() if k != "micro_ts_ms"})
        merged["micro_data_fresh"] = 1.0
        merged["micro_data_age_ms"] = age_ms
        return merged

    async def process_scan_message(self, data: str):
        try:
            scan = json.loads(data)
            if not isinstance(scan, dict):
                return
            symbol = self._infer_symbol_from_scan(scan)
            if not symbol:
                return

            timestamp_ms = self._normalize_timestamp_ms(scan.get("timestamp"))
            micro = self._extract_microstructure_features(scan)
            if not micro:
                return
            micro["micro_ts_ms"] = float(timestamp_ms)
            self.microstructure_by_symbol[symbol] = micro
        except json.JSONDecodeError as exc:
            self._record_exception("process_scan_message.json_decode", exc)
        except Exception as e:
            self._record_exception("process_scan_message", e)
            logger.error(f"Failed to process scan message: {e}")

    async def process_ticker_message(self, data: str):
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
                features.update(self._current_microstructure_features(symbol, timestamp_ms))
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
            self._record_exception("process_ticker_message.json_decode", exc)
            logger.error("Failed to decode message")
        except Exception as e:
            self._record_exception("process_ticker_message", e)
            logger.error(f"Failed to process ticker message: {e}")

    async def stop(self):
        self.running = False
        await self._close_connections()
        logger.info("Market Data Consumer stopped")
