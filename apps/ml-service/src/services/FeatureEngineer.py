from collections import deque
import logging
import math
import numpy as np

logger = logging.getLogger(__name__)

MAX_WINDOW_SIZE = 2000


class FeatureEngineer:
    def __init__(self, window_size=100):
        bounded_window = int(max(20, min(MAX_WINDOW_SIZE, window_size)))
        self.window_size = bounded_window
        self.timestamps = deque(maxlen=bounded_window)
        self.prices = deque(maxlen=bounded_window)

        # Incremental EMA/MACD state for low-latency updates.
        self.ema_12 = None
        self.ema_26 = None
        self.macd_signal = None

        self.alpha_12 = 2.0 / (12.0 + 1.0)
        self.alpha_26 = 2.0 / (26.0 + 1.0)
        self.alpha_9 = 2.0 / (9.0 + 1.0)

    def _safe_float(self, value):
        if value is None:
            return None
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return None
        return parsed

    def _latest_returns(self, prices_np):
        if prices_np.size < 2:
            return None
        prev = prices_np[-2]
        if prev <= 0:
            return None
        return (prices_np[-1] / prev) - 1.0

    def _sma(self, prices_np, window):
        if prices_np.size < window:
            return None
        return float(np.mean(prices_np[-window:]))

    def _rsi_14(self, prices_np):
        if prices_np.size < 15:
            return None
        deltas = np.diff(prices_np[-15:])
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_gain = float(np.mean(gains))
        avg_loss = float(np.mean(losses))
        if avg_loss <= 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _momentum_10(self, prices_np):
        if prices_np.size < 11:
            return None
        base = prices_np[-11]
        if base <= 0:
            return None
        return float((prices_np[-1] / base) - 1.0)

    def _vol_5m(self, prices_np):
        if prices_np.size < 6:
            return None
        returns = np.diff(prices_np[-6:]) / prices_np[-6:-1]
        return float(np.std(returns, ddof=1))

    def update(self, timestamp, price):
        px = float(price)
        self.timestamps.append(timestamp)
        self.prices.append(px)

        # Update EMA/MACD incrementally.
        if self.ema_12 is None:
            self.ema_12 = px
            self.ema_26 = px
            self.macd_signal = 0.0
        else:
            self.ema_12 = (self.alpha_12 * px) + ((1.0 - self.alpha_12) * self.ema_12)
            self.ema_26 = (self.alpha_26 * px) + ((1.0 - self.alpha_26) * self.ema_26)
            macd_now = self.ema_12 - self.ema_26
            self.macd_signal = (self.alpha_9 * macd_now) + ((1.0 - self.alpha_9) * self.macd_signal)

        return self.calculate_features()

    def calculate_features(self):
        if len(self.prices) < 20:
            return None

        prices_np = np.array(self.prices, dtype=np.float64)
        sma_20 = self._sma(prices_np, 20)
        bb_std = float(np.std(prices_np[-20:], ddof=1)) if prices_np.size >= 20 else None
        macd = None if self.ema_12 is None or self.ema_26 is None else (self.ema_12 - self.ema_26)

        features = {
            'price': float(prices_np[-1]),
            'sma_5': self._sma(prices_np, 5),
            'sma_20': sma_20,
            'sma_50': self._sma(prices_np, 50),
            'ema_12': self.ema_12,
            'ema_26': self.ema_26,
            'macd': macd,
            'macd_signal': self.macd_signal,
            'bb_upper': None if sma_20 is None or bb_std is None else sma_20 + (2.0 * bb_std),
            'bb_lower': None if sma_20 is None or bb_std is None else sma_20 - (2.0 * bb_std),
            'rsi_14': self._rsi_14(prices_np),
            'momentum_10': self._momentum_10(prices_np),
            'vol_5m': self._vol_5m(prices_np),
            'returns': self._latest_returns(prices_np),
        }

        for key, value in list(features.items()):
            features[key] = self._safe_float(value)

        return features
