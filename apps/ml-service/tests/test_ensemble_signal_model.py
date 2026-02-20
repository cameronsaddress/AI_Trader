import os
import shutil
import sys
import tempfile
import unittest
import numpy as np


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from services.EnsembleSignalModel import EnsembleSignalModel  # noqa: E402
from services.EnsembleSignalModel import FEATURE_NAMES  # noqa: E402


class EnsembleSignalModelTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="ml-model-test-")
        os.environ["ML_SIGNAL_STATE_DIR"] = self.temp_dir
        os.environ["ML_SIGNAL_MIN_TRAIN_SAMPLES"] = "9999"
        os.environ["ML_SIGNAL_EVAL_MIN_SAMPLES"] = "20"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.environ.pop("ML_SIGNAL_STATE_DIR", None)
        os.environ.pop("ML_SIGNAL_MIN_TRAIN_SAMPLES", None)
        os.environ.pop("ML_SIGNAL_EVAL_MIN_SAMPLES", None)

    def test_bootstrap_output_sane_before_training(self):
        model = EnsembleSignalModel("TEST_USD")
        payload = model.update(
            1_700_000_000_000,
            {
                "price": 100.0,
                "rsi_14": 52.0,
                "momentum_10": 0.01,
                "vol_5m": 0.004,
                "returns": 0.001,
                "micro_buy_pressure": 0.60,
                "micro_uptick_ratio": 0.56,
                "micro_data_fresh": 1.0,
            },
        )
        self.assertFalse(payload["ml_model_ready"])
        self.assertGreaterEqual(payload["ml_prob_up"], 0.001)
        self.assertLessEqual(payload["ml_prob_up"], 0.999)
        self.assertAlmostEqual(payload["ml_prob_up"] + payload["ml_prob_down"], 1.0, places=6)
        self.assertIn("ml_model_version", payload)
        self.assertIn("ml_feature_drift_score", payload)
        self.assertIn("ml_feature_importance_top", payload)
        self.assertIn("ml_model_walk_forward_auc", payload)

    def test_invalid_input_returns_neutral_probability(self):
        model = EnsembleSignalModel("TEST_USD")
        payload = model.update(0, {"price": 0})
        self.assertAlmostEqual(payload["ml_prob_up"], 0.5, places=6)
        self.assertEqual(payload["ml_signal_direction"], "NEUTRAL")

    def test_book_age_scaling_respects_env(self):
        os.environ["ML_MICROSTRUCTURE_BOOK_AGE_SCALE_MS"] = "5000"
        model = EnsembleSignalModel("TEST_USD")
        vector = model._build_feature_vector({
            "price": 100.0,
            "micro_book_age_ms": 5000.0,
        })
        # index 11 == book_staleness
        self.assertAlmostEqual(float(vector[11]), 1.0, places=6)
        os.environ.pop("ML_MICROSTRUCTURE_BOOK_AGE_SCALE_MS", None)

    def test_walk_forward_auc_returns_valid_probability_metric(self):
        model = EnsembleSignalModel("TEST_USD")
        feature_count = len(FEATURE_NAMES)
        rng = np.random.default_rng(seed=7)
        x = rng.normal(0.0, 1.0, size=(120, feature_count)).astype(np.float64)
        y = np.array(([0, 1] * 60), dtype=np.int32)

        auc = model._walk_forward_auc(x, y)
        self.assertIsNotNone(auc)
        assert auc is not None
        self.assertGreaterEqual(auc, 0.0)
        self.assertLessEqual(auc, 1.0)


if __name__ == "__main__":
    unittest.main()
