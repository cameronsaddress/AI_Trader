from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import logging
import math
import os
import pickle
import time
from typing import Any

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from xgboost import XGBClassifier
except Exception:  # pragma: no cover - fallback if xgboost wheel is unavailable
    XGBClassifier = None


logger = logging.getLogger(__name__)

EPS = 1e-9


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(float(raw.strip()))
        return max(lo, min(hi, parsed))
    except Exception:
        return default


def _env_float(name: str, default: float, lo: float, hi: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = float(raw.strip())
        if not math.isfinite(parsed):
            return default
        return max(lo, min(hi, parsed))
    except Exception:
        return default


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _safe_number(value: Any, fallback: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return fallback
    if not math.isfinite(parsed):
        return fallback
    return parsed


def _safe_probability(value: float) -> float:
    return _clip(_safe_number(value, 0.5), 0.001, 0.999)


def _sigmoid(value: float) -> float:
    clipped = _clip(value, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-clipped))


@dataclass
class PendingSample:
    timestamp_ms: int
    price: float
    features: np.ndarray


@dataclass
class TrainedModel:
    name: str
    estimator: Any
    weight: float


class EnsembleSignalModel:
    """
    Streaming ensemble model for directional short-horizon signal generation.

    Design:
    - Labels are derived online from realized return over a fixed time horizon.
    - Uses a robust tabular ensemble (XGBoost + RandomForest + LogisticRegression).
    - Falls back to deterministic heuristic until enough data is available.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol

        self.label_horizon_ms = _env_int("ML_SIGNAL_LABEL_HORIZON_MS", 90_000, 15_000, 600_000)
        self.label_neutral_band = _env_float("ML_SIGNAL_LABEL_NEUTRAL_BAND", 0.0005, 0.0, 0.01)
        self.min_train_samples = _env_int("ML_SIGNAL_MIN_TRAIN_SAMPLES", 400, 80, 20_000)
        self.max_train_samples = _env_int("ML_SIGNAL_MAX_TRAIN_SAMPLES", 5000, 500, 100_000)
        self.retrain_every_samples = _env_int("ML_SIGNAL_RETRAIN_EVERY_SAMPLES", 64, 8, 2048)
        self.min_retrain_interval_ms = _env_int("ML_SIGNAL_MIN_RETRAIN_INTERVAL_MS", 45_000, 5_000, 900_000)
        self.min_class_samples = _env_int("ML_SIGNAL_MIN_CLASS_SAMPLES", 40, 10, 2000)
        self.eval_min_samples = _env_int("ML_SIGNAL_EVAL_MIN_SAMPLES", 80, 20, 5000)

        self.pending_samples: deque[PendingSample] = deque(maxlen=self.max_train_samples * 2)
        self.train_features: deque[np.ndarray] = deque(maxlen=self.max_train_samples)
        self.train_labels: deque[int] = deque(maxlen=self.max_train_samples)
        self.new_labels_since_train = 0

        self.models: list[TrainedModel] = []
        self.model_name = "HEURISTIC_BOOTSTRAP"
        self.last_train_ts_ms = 0
        self.last_eval_logloss: float | None = None
        self.last_eval_brier: float | None = None
        self.last_eval_accuracy: float | None = None
        self.last_eval_auc: float | None = None
        self.exception_counts: dict[str, int] = {}
        self.last_exception_scope: str | None = None
        self.last_exception_message: str | None = None
        self.last_exception_ts_ms = 0
        self.last_exception_log_ts_ms = 0
        self.exception_log_cooldown_ms = _env_int(
            "ML_SIGNAL_EXCEPTION_LOG_COOLDOWN_MS",
            60_000,
            1_000,
            3_600_000,
        )
        self.state_persist_every_samples = _env_int(
            "ML_SIGNAL_STATE_PERSIST_EVERY_SAMPLES",
            256,
            16,
            10_000,
        )
        default_state_dir = os.path.abspath(os.path.join("reports", "models", "ml_service"))
        self.state_dir = os.getenv("ML_SIGNAL_STATE_DIR", default_state_dir)
        safe_symbol = self.symbol.replace("-", "_").replace("/", "_").strip().upper()
        self.state_path = os.path.join(self.state_dir, f"signal_model_state_{safe_symbol}.pkl")
        self._load_state()

    def _record_exception(self, scope: str, exc: Exception, now_ms: int | None = None) -> None:
        ts_ms = int(now_ms if now_ms is not None else (time.time() * 1000))
        self.exception_counts[scope] = self.exception_counts.get(scope, 0) + 1
        self.last_exception_scope = scope
        self.last_exception_message = f"{type(exc).__name__}: {exc}"[:240]
        self.last_exception_ts_ms = ts_ms
        if (
            self.last_exception_log_ts_ms <= 0
            or (ts_ms - self.last_exception_log_ts_ms) >= self.exception_log_cooldown_ms
        ):
            self.last_exception_log_ts_ms = ts_ms
            logger.warning(
                "[ML][telemetry] exception symbol=%s scope=%s count=%d msg=%s",
                self.symbol,
                scope,
                self.exception_counts[scope],
                self.last_exception_message,
            )

    def _load_state(self) -> None:
        if not self.state_path or not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, "rb") as handle:
                payload = pickle.load(handle)
            if not isinstance(payload, dict):
                return
            if str(payload.get("symbol") or "") != self.symbol:
                return

            train_features = payload.get("train_features")
            train_labels = payload.get("train_labels")
            loaded_features: list[np.ndarray] = []
            loaded_labels: list[int] = []
            if isinstance(train_features, np.ndarray) and train_features.ndim == 2:
                for row in train_features[-self.max_train_samples :]:
                    loaded_features.append(np.asarray(row, dtype=np.float64))
            elif isinstance(train_features, list):
                for row in train_features[-self.max_train_samples :]:
                    if isinstance(row, np.ndarray):
                        loaded_features.append(np.asarray(row, dtype=np.float64))
                    elif isinstance(row, (list, tuple)):
                        loaded_features.append(np.asarray(row, dtype=np.float64))

            if isinstance(train_labels, np.ndarray):
                loaded_labels = [int(v) for v in train_labels.tolist()[-self.max_train_samples :]]
            elif isinstance(train_labels, list):
                loaded_labels = [int(v) for v in train_labels[-self.max_train_samples :]]

            if loaded_features and loaded_labels:
                pair_len = min(len(loaded_features), len(loaded_labels))
                self.train_features = deque(loaded_features[-pair_len:], maxlen=self.max_train_samples)
                self.train_labels = deque(loaded_labels[-pair_len:], maxlen=self.max_train_samples)

            self.new_labels_since_train = int(payload.get("new_labels_since_train") or 0)
            self.model_name = str(payload.get("model_name") or self.model_name)
            self.last_train_ts_ms = int(payload.get("last_train_ts_ms") or 0)
            self.last_eval_logloss = payload.get("last_eval_logloss")
            self.last_eval_brier = payload.get("last_eval_brier")
            self.last_eval_accuracy = payload.get("last_eval_accuracy")
            self.last_eval_auc = payload.get("last_eval_auc")

            loaded_models: list[TrainedModel] = []
            raw_models = payload.get("models")
            if isinstance(raw_models, list):
                for entry in raw_models:
                    if not isinstance(entry, dict):
                        continue
                    estimator = entry.get("estimator")
                    if estimator is None or not hasattr(estimator, "predict_proba"):
                        continue
                    loaded_models.append(
                        TrainedModel(
                            name=str(entry.get("name") or "MODEL"),
                            estimator=estimator,
                            weight=float(_safe_number(entry.get("weight"), 0.0)),
                        )
                    )
            if loaded_models:
                weight_sum = sum(max(0.0, model.weight) for model in loaded_models)
                if weight_sum <= EPS:
                    equal_weight = 1.0 / float(len(loaded_models))
                    for model in loaded_models:
                        model.weight = equal_weight
                else:
                    for model in loaded_models:
                        model.weight = max(0.0, model.weight) / weight_sum
                self.models = loaded_models
                if self.model_name == "HEURISTIC_BOOTSTRAP":
                    self.model_name = "+".join(model.name for model in self.models)
            logger.info(
                "[ML] loaded persisted state for %s (samples=%d, models=%d)",
                self.symbol,
                len(self.train_labels),
                len(self.models),
            )
        except Exception as exc:
            self._record_exception("model_state_load", exc)

    def _persist_state(self, now_ms: int | None = None) -> None:
        if not self.state_path:
            return
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            feature_matrix = np.vstack(self.train_features) if self.train_features else np.empty((0, 0), dtype=np.float64)
            label_array = np.asarray(self.train_labels, dtype=np.int32)
            payload: dict[str, Any] = {
                "version": 1,
                "symbol": self.symbol,
                "saved_at_ms": int(now_ms if now_ms is not None else (time.time() * 1000)),
                "train_features": feature_matrix,
                "train_labels": label_array,
                "new_labels_since_train": int(self.new_labels_since_train),
                "model_name": self.model_name,
                "models": [
                    {
                        "name": model.name,
                        "weight": float(model.weight),
                        "estimator": model.estimator,
                    }
                    for model in self.models
                ],
                "last_train_ts_ms": int(self.last_train_ts_ms),
                "last_eval_logloss": self.last_eval_logloss,
                "last_eval_brier": self.last_eval_brier,
                "last_eval_accuracy": self.last_eval_accuracy,
                "last_eval_auc": self.last_eval_auc,
            }
            temp_path = f"{self.state_path}.tmp"
            with open(temp_path, "wb") as handle:
                pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(temp_path, self.state_path)
        except Exception as exc:
            self._record_exception("model_state_persist", exc, now_ms)

    def update(self, timestamp_ms: int, features: dict[str, Any]) -> dict[str, Any]:
        now_ms = int(_safe_number(timestamp_ms, 0))
        if now_ms <= 0:
            return self._build_output(0.5, now_ms)

        price = _safe_number(features.get("price"), 0.0)
        if price <= 0.0:
            return self._build_output(0.5, now_ms)

        vector = self._build_feature_vector(features)
        self.pending_samples.append(PendingSample(timestamp_ms=now_ms, price=price, features=vector))

        self._materialize_labels(now_ms, price)
        self._train_if_needed(now_ms)

        probability_up = self._predict_probability(vector)
        return self._build_output(probability_up, now_ms)

    def _build_feature_vector(self, row: dict[str, Any]) -> np.ndarray:
        price = _safe_number(row.get("price"), 0.0)
        rsi = _safe_number(row.get("rsi_14"), 50.0)
        momentum_10 = _safe_number(row.get("momentum_10"), 0.0)
        vol_5m = _safe_number(row.get("vol_5m"), 0.0)
        returns = _safe_number(row.get("returns"), 0.0)
        sma_20 = _safe_number(row.get("sma_20"), price)
        sma_50 = _safe_number(row.get("sma_50"), sma_20)
        ema_12 = _safe_number(row.get("ema_12"), price)
        ema_26 = _safe_number(row.get("ema_26"), price)
        macd = _safe_number(row.get("macd"), 0.0)
        macd_signal = _safe_number(row.get("macd_signal"), 0.0)
        bb_upper = _safe_number(row.get("bb_upper"), 0.0)
        bb_lower = _safe_number(row.get("bb_lower"), 0.0)
        micro_spread = _safe_number(row.get("micro_spread"), 0.0)
        micro_book_age_ms = _safe_number(row.get("micro_book_age_ms"), 0.0)
        micro_edge_to_spread_ratio = _safe_number(row.get("micro_edge_to_spread_ratio"), 0.0)
        micro_parity_deviation = _safe_number(row.get("micro_parity_deviation"), 0.0)
        micro_obi = _safe_number(row.get("micro_obi"), 0.0)
        micro_buy_pressure = _safe_number(row.get("micro_buy_pressure"), 0.5)
        micro_uptick_ratio = _safe_number(row.get("micro_uptick_ratio"), 0.5)
        micro_cluster_confidence = _safe_number(row.get("micro_cluster_confidence"), 0.0)
        micro_flow_acceleration = _safe_number(row.get("micro_flow_acceleration"), 0.0)
        micro_dynamic_cost_rate = _safe_number(row.get("micro_dynamic_cost_rate"), 0.0)
        micro_momentum_sigma = _safe_number(row.get("micro_momentum_sigma"), 0.0)
        micro_data_fresh = _safe_number(row.get("micro_data_fresh"), 0.0)

        rsi_centered = _clip((rsi - 50.0) / 50.0, -1.0, 1.0)
        momentum = _clip(momentum_10, -0.06, 0.06)
        realized_vol = _clip(vol_5m, 0.0, 0.12)
        latest_return = _clip(returns, -0.03, 0.03)

        sma_gap_20 = 0.0 if sma_20 <= EPS else _clip((price - sma_20) / sma_20, -0.06, 0.06)
        sma_gap_50 = 0.0 if sma_50 <= EPS else _clip((price - sma_50) / sma_50, -0.08, 0.08)
        ema_gap = 0.0 if price <= EPS else _clip((ema_12 - ema_26) / price, -0.03, 0.03)
        macd_norm = 0.0 if price <= EPS else _clip(macd / price, -0.03, 0.03)
        macd_signal_norm = 0.0 if price <= EPS else _clip(macd_signal / price, -0.03, 0.03)

        bb_position = 0.0
        band_width = bb_upper - bb_lower
        if band_width > EPS and price > 0.0:
            normalized = ((price - bb_lower) / band_width) - 0.5
            bb_position = _clip(normalized, -1.0, 1.0)

        spread_norm = _clip(micro_spread / 0.20, 0.0, 1.0)
        book_staleness = _clip(micro_book_age_ms / 15_000.0, 0.0, 1.0)
        flow_tilt = _clip((micro_buy_pressure - 0.5) * 2.0, -1.0, 1.0)
        uptick_tilt = _clip((micro_uptick_ratio - 0.5) * 2.0, -1.0, 1.0)
        cluster_tilt = _clip((micro_cluster_confidence * 2.0) - 1.0, -1.0, 1.0)
        obi_norm = _clip(micro_obi, -1.0, 1.0)
        parity_norm = _clip(micro_parity_deviation / 0.05, 0.0, 1.0)
        edge_to_spread_norm = _clip(micro_edge_to_spread_ratio / 4.0, 0.0, 1.0)
        flow_accel_norm = _clip(micro_flow_acceleration / 3.0, 0.0, 1.0)
        dynamic_cost_norm = _clip(micro_dynamic_cost_rate / 0.05, 0.0, 1.0)
        momentum_sigma_norm = _clip(micro_momentum_sigma / 0.05, 0.0, 1.0)
        micro_fresh = _clip(micro_data_fresh, 0.0, 1.0)

        return np.array(
            [
                rsi_centered,
                momentum,
                realized_vol,
                latest_return,
                sma_gap_20,
                sma_gap_50,
                ema_gap,
                macd_norm,
                macd_signal_norm,
                bb_position,
                spread_norm,
                book_staleness,
                flow_tilt,
                uptick_tilt,
                cluster_tilt,
                obi_norm,
                parity_norm,
                edge_to_spread_norm,
                flow_accel_norm,
                dynamic_cost_norm,
                momentum_sigma_norm,
                micro_fresh,
            ],
            dtype=np.float64,
        )

    def _materialize_labels(self, now_ms: int, now_price: float) -> None:
        while self.pending_samples and (now_ms - self.pending_samples[0].timestamp_ms) >= self.label_horizon_ms:
            old = self.pending_samples.popleft()
            if old.price <= 0.0:
                continue

            realized_return = (now_price / old.price) - 1.0
            if not math.isfinite(realized_return):
                continue
            if abs(realized_return) < self.label_neutral_band:
                continue

            label = 1 if realized_return > 0.0 else 0
            self.train_features.append(old.features)
            self.train_labels.append(label)
            self.new_labels_since_train += 1
            if self.new_labels_since_train % self.state_persist_every_samples == 0:
                self._persist_state(now_ms)

    def _train_if_needed(self, now_ms: int) -> None:
        sample_count = len(self.train_labels)
        if sample_count < self.min_train_samples:
            return

        if self.last_train_ts_ms > 0 and self.new_labels_since_train < self.retrain_every_samples:
            return

        if self.last_train_ts_ms > 0 and (now_ms - self.last_train_ts_ms) < self.min_retrain_interval_ms:
            return

        x = np.vstack(self.train_features)
        y = np.asarray(self.train_labels, dtype=np.int32)
        positives = int(y.sum())
        negatives = int(y.shape[0] - positives)
        if positives < self.min_class_samples or negatives < self.min_class_samples:
            return

        eval_size = max(self.eval_min_samples, int(y.shape[0] * 0.20))
        if y.shape[0] <= eval_size + self.min_class_samples:
            return
        split_idx = y.shape[0] - eval_size

        x_train = x[:split_idx]
        y_train = y[:split_idx]
        x_eval = x[split_idx:]
        y_eval = y[split_idx:]

        if len(np.unique(y_train)) < 2:
            return

        candidates: list[tuple[str, Any]] = [
            (
                "LR",
                Pipeline(
                    steps=[
                        ("scaler", StandardScaler()),
                        (
                            "model",
                            LogisticRegression(
                                max_iter=600,
                                class_weight="balanced",
                                C=0.85,
                                solver="lbfgs",
                            ),
                        ),
                    ]
                ),
            ),
            (
                "RF",
                RandomForestClassifier(
                    n_estimators=240,
                    max_depth=7,
                    min_samples_leaf=10,
                    class_weight="balanced_subsample",
                    random_state=42,
                    n_jobs=1,
                ),
            ),
        ]

        if XGBClassifier is not None:
            candidates.append(
                (
                    "XGB",
                    XGBClassifier(
                        n_estimators=260,
                        max_depth=4,
                        learning_rate=0.045,
                        subsample=0.90,
                        colsample_bytree=0.85,
                        min_child_weight=6,
                        gamma=0.0,
                        reg_alpha=0.15,
                        reg_lambda=1.4,
                        objective="binary:logistic",
                        eval_metric="logloss",
                        tree_method="hist",
                        random_state=42,
                        n_jobs=1,
                    ),
                )
            )

        trained: list[dict[str, Any]] = []
        for name, estimator in candidates:
            try:
                estimator.fit(x_train, y_train)
                proba_eval = estimator.predict_proba(x_eval)[:, 1].astype(np.float64)
                metrics = self._evaluate(y_eval, proba_eval)
                quality = self._quality(metrics)
                trained.append(
                    {
                        "name": name,
                        "estimator": estimator,
                        "quality": quality,
                        "proba_eval": proba_eval,
                        "metrics": metrics,
                    }
                )
            except Exception as exc:
                self._record_exception("train_model", exc, now_ms)
                logger.warning("Model %s failed during training for %s: %s", name, self.symbol, exc)

        if not trained:
            return

        quality_sum = sum(max(0.01, _safe_number(item["quality"], 0.01)) for item in trained)
        for item in trained:
            item["weight"] = max(0.01, _safe_number(item["quality"], 0.01)) / quality_sum

        ensemble_eval = np.zeros_like(trained[0]["proba_eval"], dtype=np.float64)
        for item in trained:
            ensemble_eval += float(item["weight"]) * item["proba_eval"]
        ensemble_metrics = self._evaluate(y_eval, ensemble_eval)

        self.models = [
            TrainedModel(
                name=str(item["name"]),
                estimator=item["estimator"],
                weight=float(item["weight"]),
            )
            for item in trained
        ]
        self.model_name = "+".join(model.name for model in self.models)
        self.last_train_ts_ms = now_ms
        self.new_labels_since_train = 0
        self.last_eval_logloss = ensemble_metrics.get("logloss")
        self.last_eval_brier = ensemble_metrics.get("brier")
        self.last_eval_accuracy = ensemble_metrics.get("accuracy")
        self.last_eval_auc = ensemble_metrics.get("auc")
        self._persist_state(now_ms)

        logger.info(
            "[ML] %s trained %s with %d samples (eval=%d, auc=%s, logloss=%s)",
            self.symbol,
            self.model_name,
            y.shape[0],
            y_eval.shape[0],
            f"{self.last_eval_auc:.4f}" if self.last_eval_auc is not None else "n/a",
            f"{self.last_eval_logloss:.4f}" if self.last_eval_logloss is not None else "n/a",
        )

    def _evaluate(self, y_true: np.ndarray, proba: np.ndarray) -> dict[str, float | None]:
        clipped = np.clip(proba.astype(np.float64), 0.001, 0.999)
        pred = (clipped >= 0.5).astype(np.int32)
        metrics: dict[str, float | None] = {
            "accuracy": float(accuracy_score(y_true, pred)),
            "brier": float(brier_score_loss(y_true, clipped)),
            "logloss": float(log_loss(y_true, clipped, labels=[0, 1])),
            "auc": None,
        }
        if len(np.unique(y_true)) >= 2:
            try:
                metrics["auc"] = float(roc_auc_score(y_true, clipped))
            except ValueError as exc:
                self._record_exception("evaluate_auc", exc)
                metrics["auc"] = None
        return metrics

    def _quality(self, metrics: dict[str, float | None]) -> float:
        auc = metrics.get("auc")
        logloss_value = metrics.get("logloss")
        brier = metrics.get("brier")
        accuracy = metrics.get("accuracy")

        quality = 0.0
        if auc is not None:
            quality += max(0.0, auc - 0.50) * 3.0
        if logloss_value is not None:
            quality += max(0.0, 0.72 - logloss_value) * 2.0
        if brier is not None:
            quality += max(0.0, 0.30 - brier) * 2.0
        if accuracy is not None:
            quality += max(0.0, accuracy - 0.50) * 1.5
        return max(0.01, quality)

    def _predict_probability(self, vector: np.ndarray) -> float:
        if self.models:
            weighted = 0.0
            weight_sum = 0.0
            batch = vector.reshape(1, -1)
            for model in self.models:
                try:
                    proba = float(model.estimator.predict_proba(batch)[0, 1])
                    if not math.isfinite(proba):
                        continue
                    weighted += model.weight * _safe_probability(proba)
                    weight_sum += model.weight
                except Exception as exc:
                    self._record_exception("predict_probability", exc)
                    continue
            if weight_sum > 0:
                return _safe_probability(weighted / weight_sum)

        return self._bootstrap_probability(vector)

    def _bootstrap_probability(self, vector: np.ndarray) -> float:
        # Conservative heuristic for warm-up period before supervised labels are available.
        score = (
            0.95 * vector[0]   # RSI mean-reversion vs momentum bias
            + 1.35 * vector[1]  # 10-step momentum
            - 0.90 * vector[2]  # realized volatility drag
            + 1.20 * vector[3]  # latest return continuation
            + 0.70 * vector[4]  # distance vs SMA20
            + 0.35 * vector[6]  # EMA spread
            + 0.45 * vector[7]  # MACD normalized
            - 0.30 * vector[10]  # spread friction
            - 0.25 * vector[11]  # stale book penalty
            + 0.45 * vector[12]  # buy pressure
            + 0.30 * vector[13]  # uptick confirmation
            + 0.25 * vector[14]  # cluster confidence
            + 0.35 * vector[15]  # order-book imbalance
            - 0.20 * vector[16]  # parity instability
            + 0.20 * vector[17]  # edge/spread efficiency
            + 0.10 * vector[18]  # accelerating flow
            - 0.15 * vector[19]  # dynamic cost drag
            - 0.10 * vector[20]  # sigma expansion risk
            + 0.20 * vector[21]  # microstructure freshness bonus
        )
        return _safe_probability(_sigmoid(score))

    def _build_output(self, prob_up: float, now_ms: int) -> dict[str, Any]:
        probability_up = _safe_probability(prob_up)
        edge = _clip((probability_up - 0.5) * 2.0, -1.0, 1.0)
        confidence = abs(edge)
        model_age_ms: int | None = None
        if self.last_train_ts_ms > 0 and now_ms > 0:
            model_age_ms = max(0, now_ms - self.last_train_ts_ms)

        output: dict[str, Any] = {
            "ml_prob_up": float(probability_up),
            "ml_prob_down": float(1.0 - probability_up),
            "ml_signal_edge": float(edge),
            "ml_signal_confidence": float(confidence),
            "ml_signal_direction": "UP" if edge > 0.0 else ("DOWN" if edge < 0.0 else "NEUTRAL"),
            "ml_model": self.model_name,
            "ml_model_ready": bool(self.models),
            "ml_model_samples": int(len(self.train_labels)),
            "ml_model_horizon_ms": int(self.label_horizon_ms),
            "ml_model_last_train_ts": int(self.last_train_ts_ms) if self.last_train_ts_ms > 0 else None,
            "ml_model_age_ms": model_age_ms,
            "ml_model_eval_logloss": self.last_eval_logloss,
            "ml_model_eval_auc": self.last_eval_auc,
            "ml_model_eval_accuracy": self.last_eval_accuracy,
            "ml_model_eval_brier": self.last_eval_brier,
            "ml_telemetry_exception_total": int(sum(self.exception_counts.values())),
            "ml_telemetry_last_exception_scope": self.last_exception_scope,
            "ml_telemetry_last_exception_message": self.last_exception_message,
            "ml_telemetry_last_exception_ts": int(self.last_exception_ts_ms) if self.last_exception_ts_ms > 0 else None,
        }

        for key, value in list(output.items()):
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                output[key] = None
        return output
