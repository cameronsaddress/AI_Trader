import json
import os
import sys
import types
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Allow importing MarketDataConsumer in minimal test environments where
# redis-py may not be installed.
if "redis.asyncio" not in sys.modules:
    redis_module = types.ModuleType("redis")
    redis_asyncio_module = types.ModuleType("redis.asyncio")
    redis_asyncio_module.from_url = lambda *_args, **_kwargs: None
    redis_module.asyncio = redis_asyncio_module
    sys.modules["redis"] = redis_module
    sys.modules["redis.asyncio"] = redis_asyncio_module

from services.MarketDataConsumer import MarketDataConsumer  # noqa: E402


class FakeFeatureEngineer:
    def __init__(self):
        self.calls: list[tuple[int, float]] = []

    def update(self, timestamp_ms: int, price: float):
        self.calls.append((timestamp_ms, price))
        return {
            "price_marker": float(price),
            "returns": float(price) / 1000.0,
            "rsi_14": 50.0,
            "sma_20": float(price),
        }


class CapturingSignalModel:
    def __init__(self):
        self.calls: list[tuple[int, dict[str, float]]] = []

    def update(self, timestamp_ms: int, features: dict[str, float]):
        self.calls.append((timestamp_ms, dict(features)))
        return {
            "ml_prob_up": 0.62,
            "ml_prob_down": 0.38,
            "ml_signal_confidence": 0.24,
            "ml_signal_direction": "UP",
            "ml_model": "TEST",
        }


class FakeRedis:
    def __init__(self):
        self.messages: list[tuple[str, dict[str, object]]] = []

    async def publish(self, channel: str, payload: str):
        self.messages.append((channel, json.loads(payload)))
        return 1


class MarketDataConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_ticker_pipeline_uses_lagged_technical_features(self):
        consumer = MarketDataConsumer()
        symbol = "BTC-USD"
        fake_engineer = FakeFeatureEngineer()
        fake_model = CapturingSignalModel()
        fake_redis = FakeRedis()
        consumer.feature_engineers[symbol] = fake_engineer
        consumer.signal_models[symbol] = fake_model
        consumer.redis = fake_redis

        await consumer.process_scan_message(json.dumps({
            "strategy": "BTC_5M",
            "timestamp": 1_700_000_000_000,
            "meta": {
                "spot": 100.0,
                "window_start_spot": 100.0,
                "seconds_to_expiry": 260,
                "window_seconds": 300,
                "fair_yes": 0.58,
                "iv_annualized": 0.72,
                "yes_spread": 0.01,
                "no_spread": 0.02,
            },
        }))

        # First ticker only primes lag state to avoid same-tick leakage.
        await consumer.process_ticker_message(json.dumps({
            "symbol": symbol,
            "timestamp": 1_700_000_000_000,
            "price": 100.0,
        }))
        self.assertEqual(len(fake_model.calls), 0)
        self.assertEqual(len(fake_redis.messages), 0)

        # Second ticker should use the prior feature snapshot (price_marker=100.0).
        await consumer.process_ticker_message(json.dumps({
            "symbol": symbol,
            "timestamp": 1_700_000_001_000,
            "price": 120.0,
        }))
        self.assertEqual(len(fake_model.calls), 1)
        used_features = fake_model.calls[0][1]
        self.assertAlmostEqual(float(used_features["price_marker"]), 100.0, places=6)
        self.assertEqual(float(used_features["micro_data_fresh"]), 1.0)
        self.assertIn("micro_spot_strike_delta", used_features)
        self.assertIn("micro_time_remaining_norm", used_features)
        self.assertIn("micro_fair_yes_centered", used_features)
        self.assertIn("micro_iv_annualized", used_features)
        self.assertEqual(len(fake_redis.messages), 1)
        self.assertEqual(fake_redis.messages[0][0], "ml:features")

    def test_extract_microstructure_promotes_binary_core_features(self):
        consumer = MarketDataConsumer()
        features = consumer._extract_microstructure_features({
            "strategy": "BTC_5M",
            "timestamp": 1_700_000_000_000,
            "sum": 0.63,
            "meta": {
                "spot": 102.0,
                "window_start_spot": 100.0,
                "seconds_to_expiry": 150,
                "window_seconds": 300,
                "iv_annualized": 0.80,
                "yes_spread": 0.02,
                "no_spread": 0.03,
            },
        })

        self.assertAlmostEqual(features["micro_spot_strike_delta"], 0.02, places=6)
        self.assertAlmostEqual(features["micro_time_remaining_norm"], 0.5, places=6)
        self.assertAlmostEqual(features["micro_fair_yes_centered"], 0.26, places=6)
        self.assertAlmostEqual(features["micro_iv_annualized"], 0.80, places=6)
        self.assertAlmostEqual(features["micro_spread"], 0.025, places=6)


if __name__ == "__main__":
    unittest.main()
