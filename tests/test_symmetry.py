import numpy as np
import pytest

from src.model.features import FEATURE_COLS, swap_features, symmetrize
from src.model.predictor import MODEL_NAMES, Calibrator, Predictor

col = FEATURE_COLS.index


def test_swap_is_involution(synthetic_features):
    X = synthetic_features[FEATURE_COLS].to_numpy(float)
    np.testing.assert_allclose(swap_features(swap_features(X)), X)


def test_swap_semantics(synthetic_features):
    X = synthetic_features[FEATURE_COLS].to_numpy(float)
    Xs = swap_features(X)
    np.testing.assert_allclose(Xs[:, col("team1_elo")], X[:, col("team2_elo")])
    np.testing.assert_allclose(Xs[:, col("elo_diff")], -X[:, col("elo_diff")])
    np.testing.assert_allclose(Xs[:, col("h2h_wr")], 1 - X[:, col("h2h_wr")])
    np.testing.assert_allclose(Xs[:, col("tier")], X[:, col("tier")])  # context unchanged


def test_every_team1_column_has_a_team2_pair():
    for c in FEATURE_COLS:
        if c.startswith("team1_"):
            assert "team2_" + c[6:] in FEATURE_COLS


def test_symmetrize_flips_target(synthetic_features):
    X = synthetic_features[FEATURE_COLS].to_numpy(float)
    y = synthetic_features["team1_win"].to_numpy()
    Xs, ys = symmetrize(X, y)
    assert len(Xs) == 2 * len(X)
    assert ys.mean() == pytest.approx(0.5)
    np.testing.assert_array_equal(ys[len(y):], 1 - y)


@pytest.mark.parametrize("name", MODEL_NAMES)
def test_symmetric_predictor_is_order_invariant(synthetic_features, name):
    X = synthetic_features[FEATURE_COLS].to_numpy(float)
    y = synthetic_features["team1_win"].to_numpy()
    pred = Predictor.fit(name, X[:2000], y[:2000], symmetric=True)
    p = pred.predict_proba(X[2000:])
    p_swapped = pred.predict_proba(swap_features(X[2000:]))
    np.testing.assert_allclose(p + p_swapped, 1.0, atol=1e-9)


def test_asymmetric_predictor_learns_order_bias(synthetic_features):
    """Sanity check that the fixture's bias is real and symmetric training removes it."""
    X = synthetic_features[FEATURE_COLS].to_numpy(float)
    y = synthetic_features["team1_win"].to_numpy()
    equal = X.copy()
    equal[:, col("team2_elo")] = equal[:, col("team1_elo")]
    equal[:, col("elo_diff")] = 0.0
    raw = Predictor.fit("logistic_regression", X, y, symmetric=False)
    sym = Predictor.fit("logistic_regression", X, y, symmetric=True)
    assert raw.predict_proba(equal).mean() > 0.53
    assert sym.predict_proba(equal).mean() == pytest.approx(0.5, abs=0.02)


@pytest.mark.parametrize("method", ["platt", "isotonic"])
def test_calibrated_predictor_stays_symmetric(synthetic_features, method):
    X = synthetic_features[FEATURE_COLS].to_numpy(float)
    y = synthetic_features["team1_win"].to_numpy()
    pred = Predictor.fit("logistic_regression", X[:2000], y[:2000])
    pred.calibrator = Calibrator(method).fit(pred.predict_proba_uncalibrated(X[2000:2500]), y[2000:2500])
    p = pred.predict_proba(X[2500:])
    assert np.all((p > 0) & (p < 1))
    np.testing.assert_allclose(p + pred.predict_proba(swap_features(X[2500:])), 1.0, atol=1e-9)


def test_calibrator_is_monotone():
    rng = np.random.default_rng(1)
    p = rng.uniform(0.05, 0.95, 4000)
    y = (rng.uniform(size=4000) < p ** 1.5).astype(int)  # miscalibrated on purpose
    grid = np.linspace(0.05, 0.95, 50)
    for method in ("platt", "isotonic"):
        q = Calibrator(method).fit(p, y).transform(grid)
        assert np.all(np.diff(q) >= -1e-12)
