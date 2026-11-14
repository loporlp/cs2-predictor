"""
Model bundle used everywhere a win probability is needed.

A `Predictor` wraps an sklearn-style classifier together with its (optional)
scaler and calibrator, and exposes `predict_proba(X) -> P(team1 wins)`.
With `symmetric=True` it averages P(A beats B) with 1 - P(B beats A), so the
result never depends on which team is passed as team1.
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from src.model.features import swap_features, symmetrize

MODEL_NAMES = ["logistic_regression", "random_forest", "xgboost", "gradient_boosting"]
SCALED_MODELS = {"logistic_regression"}
CALIBRATION_METHODS = ["none", "platt", "isotonic"]

_EPS = 1e-6


def make_model(name: str):
    if name == "logistic_regression":
        return LogisticRegression(max_iter=1000, random_state=42)
    if name == "random_forest":
        return RandomForestClassifier(
            n_estimators=200, max_depth=10, min_samples_leaf=20, random_state=42, n_jobs=-1
        )
    if name == "xgboost":
        return XGBClassifier(
            n_estimators=200, max_depth=6, learning_rate=0.1,
            min_child_weight=20, random_state=42, eval_metric="logloss", verbosity=0,
        )
    if name == "gradient_boosting":
        return GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.1,
            min_samples_leaf=20, random_state=42,
        )
    raise ValueError(f"Unknown model: {name}")


class Calibrator:
    """Post-hoc probability calibration (Platt or isotonic) on P(team1 wins).

    Fitted on symmetrized (p, y) pairs so the mapping is (near) symmetric;
    `Predictor` enforces exact symmetry by averaging f(p) and 1 - f(1 - p).
    """

    def __init__(self, method: str):
        if method not in ("platt", "isotonic"):
            raise ValueError(f"Unknown calibration method: {method}")
        self.method = method
        self._model = None

    def fit(self, p, y):
        p = np.clip(np.asarray(p, dtype=float), _EPS, 1 - _EPS)
        y = np.asarray(y, dtype=int)
        p = np.concatenate([p, 1 - p])
        y = np.concatenate([y, 1 - y])
        if self.method == "platt":
            self._model = LogisticRegression().fit(_logit(p).reshape(-1, 1), y)
        else:
            self._model = IsotonicRegression(y_min=_EPS, y_max=1 - _EPS, out_of_bounds="clip").fit(p, y)
        return self

    def transform(self, p):
        p = np.clip(np.asarray(p, dtype=float), _EPS, 1 - _EPS)
        if self.method == "platt":
            return self._model.predict_proba(_logit(p).reshape(-1, 1))[:, 1]
        return np.clip(self._model.predict(p), _EPS, 1 - _EPS)


def _logit(p):
    return np.log(p / (1 - p))


@dataclass
class Predictor:
    name: str
    model: object
    scaler: Optional[StandardScaler] = None
    calibrator: Optional[Calibrator] = None
    symmetric: bool = True

    @classmethod
    def fit(cls, name: str, X, y, symmetric: bool = True) -> "Predictor":
        """Fit a fresh model. With symmetric=True, trains on both team orders."""
        if symmetric:
            X, y = symmetrize(X, y)
        scaler = StandardScaler().fit(X) if name in SCALED_MODELS else None
        model = make_model(name)
        model.fit(scaler.transform(X) if scaler is not None else X, y)
        return cls(name=name, model=model, scaler=scaler, symmetric=symmetric)

    def _raw(self, X):
        X = np.asarray(X, dtype=float)
        if self.scaler is not None:
            X = self.scaler.transform(X)
        return self.model.predict_proba(X)[:, 1]

    def predict_proba_uncalibrated(self, X):
        p = self._raw(X)
        if self.symmetric:
            p = 0.5 * (p + 1.0 - self._raw(swap_features(X)))
        return p

    def predict_proba(self, X):
        """P(team1 wins) for each row of X."""
        p = self.predict_proba_uncalibrated(X)
        if self.calibrator is not None:
            q = self.calibrator.transform(p)
            if self.symmetric:
                q = 0.5 * (q + 1.0 - self.calibrator.transform(1.0 - p))
            p = q
        return p

    @property
    def calibration_method(self) -> str:
        return self.calibrator.method if self.calibrator is not None else "none"

    def feature_importances(self, feature_cols):
        if hasattr(self.model, "feature_importances_"):
            imp = self.model.feature_importances_
        else:
            imp = np.abs(self.model.coef_[0])
        return {c: float(v) for c, v in zip(feature_cols, imp)}
