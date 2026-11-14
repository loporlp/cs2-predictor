import numpy as np
import pandas as pd
import pytest

from src.model.backtest import run_backtest, select_calibration, walk_forward_folds


def test_folds_are_chronological_and_leak_free(synthetic_features):
    dates = synthetic_features["date"]
    folds = walk_forward_folds(dates, n_folds=4, test_days=200, min_train_rows=100)
    assert len(folds) == 4
    seen_test = set()
    for f in folds:
        assert dates.iloc[f.train_idx].max() < dates.iloc[f.test_idx].min()
        assert not seen_test & set(f.test_idx), "test windows must not overlap"
        seen_test |= set(f.test_idx)
    # Test windows are consecutive and expanding-window training grows
    sizes = [len(f.train_idx) for f in folds]
    assert sizes == sorted(sizes)
    assert dates.iloc[folds[-1].test_idx].max() == dates.max()


def test_window_months_limits_training_rows(synthetic_features):
    dates = synthetic_features["date"]
    full = walk_forward_folds(dates, n_folds=2, test_days=100, min_train_rows=10)
    windowed = walk_forward_folds(dates, n_folds=2, test_days=100, window_months=1, min_train_rows=10)
    for f_full, f_win in zip(full, windowed):
        assert len(f_win.train_idx) < len(f_full.train_idx)
        span = dates.iloc[f_win.test_idx].min() - dates.iloc[f_win.train_idx].min()
        assert span <= pd.Timedelta(days=31)


def test_folds_with_too_little_training_data_are_dropped(synthetic_features):
    folds = walk_forward_folds(synthetic_features["date"], n_folds=3, test_days=600, min_train_rows=10_000)
    assert folds == []


def test_run_backtest_reports_per_fold_and_mean(synthetic_features):
    bt = run_backtest(synthetic_features, ["logistic_regression"], n_folds=3, test_days=150,
                      min_train_rows=100)
    assert len(bt.folds) == 3
    m = bt.mean_metrics["logistic_regression"]
    assert m["log_loss"] == pytest.approx(np.mean([f["metrics"]["logistic_regression"]["log_loss"] for f in bt.folds]))
    assert len(bt.oof_y) == bt.test_size == len(bt.oof_proba["logistic_regression"])
    assert m["roc_auc"] > 0.6  # the fixture has real signal


def test_select_calibration_picks_isotonic_for_miscalibrated_probs():
    rng = np.random.default_rng(2)
    true_p = rng.uniform(0.1, 0.9, 6000)
    y = (rng.uniform(size=6000) < true_p).astype(int)
    overconfident = 1 / (1 + np.exp(-3 * np.log(true_p / (1 - true_p))))
    folds = np.repeat(np.arange(3), 2000)
    res = select_calibration(overconfident, y, folds)
    assert res["method"] in ("platt", "isotonic")
    assert res["log_loss"][res["method"]] < res["log_loss"]["none"]


def test_select_calibration_keeps_none_for_calibrated_probs():
    rng = np.random.default_rng(3)
    p = rng.uniform(0.1, 0.9, 6000)
    y = (rng.uniform(size=6000) < p).astype(int)
    res = select_calibration(p, y, np.repeat(np.arange(3), 2000))
    assert res["log_loss"]["none"] <= res["log_loss"]["isotonic"]


def test_test_span_longer_than_data_is_shrunk(synthetic_features):
    """A short training window (e.g. UI '3 months') must still yield folds."""
    df = synthetic_features.iloc[-400:].reset_index(drop=True)  # ~100 days of data
    folds = walk_forward_folds(df["date"], n_folds=5, test_days=300, min_train_rows=50)
    assert folds, "expected the test span to shrink to fit the data"
    for f in folds:
        assert df["date"].iloc[f.train_idx].max() < df["date"].iloc[f.test_idx].min()
