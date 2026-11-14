"""
Walk-forward (rolling-origin) backtesting.

The last `test_days` of the feature matrix are cut into `n_folds` consecutive,
equal-length test windows. For each fold the models are trained only on rows
strictly before that fold's test window (optionally limited to the last
`window_months`), then evaluated on it. Averaging across folds gives a much
less noisy model-selection signal than one 80/20 split.

Also hosts the training-window search (previously src/experiments/window_search.py),
which now runs the same walk-forward backtest for each candidate window.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

from src.model.evaluate import evaluate_probabilities
from src.model.features import feature_frame_to_xy
from src.model.predictor import CALIBRATION_METHODS, MODEL_NAMES, Calibrator, Predictor
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.model.backtest")

FEATURE_MATRIX_PATH = "data/processed/feature_matrix.csv"
WINDOWS_MONTHS = [3, 6, 9, 12, 18, 24, None]  # None = all data before each fold


@dataclass
class Fold:
    index: int
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    train_idx: np.ndarray
    test_idx: np.ndarray


@dataclass
class BacktestResult:
    folds: List[dict]
    mean_metrics: Dict[str, dict]
    # Out-of-fold predictions per model (uncalibrated), aligned with oof_y / oof_fold
    oof_proba: Dict[str, np.ndarray] = field(default_factory=dict)
    oof_y: np.ndarray = None
    oof_fold: np.ndarray = None

    @property
    def best_model(self) -> str:
        return min(self.mean_metrics, key=lambda k: self.mean_metrics[k]["log_loss"])

    @property
    def test_period(self):
        return [self.folds[0]["test_period"][0], self.folds[-1]["test_period"][1]]

    @property
    def test_size(self) -> int:
        return int(sum(f["test_size"] for f in self.folds))


def load_feature_matrix(path: str = FEATURE_MATRIX_PATH) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date", kind="stable").reset_index(drop=True)


def walk_forward_folds(
    dates: pd.Series,
    n_folds: int = 5,
    test_days: int = 300,
    window_months: Optional[int] = None,
    min_train_rows: int = 500,
) -> List[Fold]:
    """Build chronological folds over the last `test_days` days of `dates`.

    Each fold's training rows are strictly earlier than its test window.
    Folds with fewer than `min_train_rows` training rows are dropped.
    """
    if n_folds < 1:
        raise ValueError("n_folds must be >= 1")
    dates = pd.to_datetime(pd.Series(dates)).reset_index(drop=True)
    end = dates.max()
    span = (end - dates.min()).days
    if test_days >= span:
        shrunk = max(n_folds, span // 2)
        logger.warning(f"test_days={test_days} exceeds data span ({span}d); using {shrunk}d")
        test_days = shrunk

    fold_len = pd.Timedelta(days=test_days / n_folds)
    first_start = end - pd.Timedelta(days=test_days)
    folds = []
    for k in range(n_folds):
        t0 = first_start + k * fold_len
        t1 = first_start + (k + 1) * fold_len
        last = k == n_folds - 1
        test_mask = (dates >= t0) & ((dates <= t1) if last else (dates < t1))
        train_mask = dates < t0
        if window_months is not None:
            train_mask &= dates >= t0 - pd.Timedelta(days=30 * window_months)
        train_idx = np.flatnonzero(train_mask.to_numpy())
        test_idx = np.flatnonzero(test_mask.to_numpy())
        if len(train_idx) < min_train_rows or len(test_idx) == 0:
            logger.info(
                f"Skipping fold {k + 1}: {len(train_idx)} train / {len(test_idx)} test rows"
            )
            continue
        folds.append(Fold(k, t0, t1, train_idx, test_idx))
    return folds


def run_backtest(
    df: pd.DataFrame,
    model_names: List[str] = MODEL_NAMES,
    n_folds: int = 5,
    test_days: int = 300,
    window_months: Optional[int] = None,
    symmetric: bool = True,
    min_train_rows: int = 500,
) -> BacktestResult:
    """Walk-forward backtest of each model. `df` must be sorted by date."""
    X, y = feature_frame_to_xy(df)
    folds = walk_forward_folds(df["date"], n_folds, test_days, window_months, min_train_rows)
    if not folds:
        raise ValueError(
            "No usable backtest folds — not enough data for the requested "
            f"n_folds={n_folds}, test_days={test_days}, window_months={window_months}."
        )

    fold_reports = []
    oof = {name: [] for name in model_names}
    oof_y, oof_fold = [], []

    for fold in folds:
        X_tr, y_tr = X[fold.train_idx], y[fold.train_idx]
        X_te, y_te = X[fold.test_idx], y[fold.test_idx]
        period = [str(df["date"].iloc[fold.test_idx[0]]), str(df["date"].iloc[fold.test_idx[-1]])]
        logger.info(
            f"Fold {fold.index + 1}/{n_folds}: train={len(X_tr)} test={len(X_te)} "
            f"({period[0][:10]} → {period[1][:10]})"
        )
        metrics = {}
        for name in model_names:
            pred = Predictor.fit(name, X_tr, y_tr, symmetric=symmetric)
            p = pred.predict_proba(X_te)
            metrics[name] = evaluate_probabilities(y_te, p)
            oof[name].append(p)
            logger.info(
                f"    {name:22s} log_loss={metrics[name]['log_loss']:.4f} "
                f"acc={metrics[name]['accuracy']:.4f} auc={metrics[name]['roc_auc']:.4f}"
            )
        oof_y.append(y_te)
        oof_fold.append(np.full(len(y_te), fold.index))
        fold_reports.append({
            "fold": fold.index + 1,
            "train_size": int(len(X_tr)),
            "train_start": str(df["date"].iloc[fold.train_idx[0]]),
            "test_size": int(len(X_te)),
            "test_period": period,
            "metrics": metrics,
        })

    mean_metrics = {}
    for name in model_names:
        per_fold = [f["metrics"][name] for f in fold_reports]
        agg = {k: float(np.mean([m[k] for m in per_fold])) for k in per_fold[0]}
        agg["log_loss_std"] = float(np.std([m["log_loss"] for m in per_fold]))
        mean_metrics[name] = agg

    return BacktestResult(
        folds=fold_reports,
        mean_metrics=mean_metrics,
        oof_proba={k: np.concatenate(v) for k, v in oof.items()},
        oof_y=np.concatenate(oof_y),
        oof_fold=np.concatenate(oof_fold),
    )


def select_calibration(p: np.ndarray, y: np.ndarray, fold_ids: np.ndarray) -> dict:
    """Pick a calibration method using rolling out-of-fold evaluation.

    For each fold k (after the first), a calibrator is fit on the
    out-of-fold predictions of folds < k and scored on fold k, so the
    calibrator is never evaluated on data it was fit on.
    """
    unique = np.unique(fold_ids)
    if len(unique) < 2:
        return {"method": "none", "log_loss": {}, "reason": "need >= 2 folds"}

    scores = {m: [] for m in CALIBRATION_METHODS}
    for k in unique[1:]:
        fit_mask, eval_mask = fold_ids < k, fold_ids == k
        for method in CALIBRATION_METHODS:
            if method == "none":
                q = p[eval_mask]
            else:
                cal = Calibrator(method).fit(p[fit_mask], y[fit_mask])
                pe = p[eval_mask]
                q = 0.5 * (cal.transform(pe) + 1 - cal.transform(1 - pe))
            scores[method].append(log_loss(y[eval_mask], np.clip(q, 1e-6, 1 - 1e-6), labels=[0, 1]))

    mean = {m: float(np.mean(v)) for m, v in scores.items()}
    best = min(mean, key=mean.get)
    # Only adopt a calibrator if it actually beats the raw model
    if best != "none" and mean[best] >= mean["none"]:
        best = "none"
    return {"method": best, "log_loss": mean}


# ---------------------------------------------------------------------------
# Training window search
# ---------------------------------------------------------------------------

def run_window_search(test_days: int = 90, n_folds: int = 3, min_train_rows: int = 500,
                      model_names: List[str] = MODEL_NAMES, symmetric: bool = True,
                      record: bool = True) -> dict:
    """Compare training-window lengths with a walk-forward backtest.

    Every window is scored on the same `n_folds` test folds covering the last
    `test_days` days, so log losses are directly comparable.
    """
    logger.info("=" * 60)
    logger.info("TRAINING WINDOW SEARCH (walk-forward)")
    logger.info("=" * 60)

    df = load_feature_matrix()
    logger.info(f"Loaded feature matrix: {len(df)} rows ({df['date'].min().date()} → {df['date'].max().date()})")

    window_results = []
    test_period, test_size = None, None
    for window_months in WINDOWS_MONTHS:
        label = "All data" if window_months is None else f"{window_months}m"
        logger.info(f"--- Window {label} ---")
        try:
            bt = run_backtest(df, model_names, n_folds, test_days, window_months,
                              symmetric=symmetric, min_train_rows=min_train_rows)
        except ValueError as exc:
            logger.info(f"Skipping window={label}: {exc}")
            continue
        if len(bt.folds) < n_folds:
            logger.info(f"Skipping window={label}: only {len(bt.folds)}/{n_folds} folds had enough data")
            continue
        test_period, test_size = bt.test_period, bt.test_size
        best = bt.best_model
        window_results.append({
            "window": label,
            "train_rows": int(np.mean([f["train_size"] for f in bt.folds])),
            "train_start": bt.folds[0]["train_start"][:10],
            "models": {
                name: {k: round(v, 4) for k, v in m.items()}
                for name, m in bt.mean_metrics.items()
            },
            "best_model": best,
        })
        m = bt.mean_metrics[best]
        logger.info(f"  best={best} log_loss={m['log_loss']:.4f} (±{m['log_loss_std']:.4f})")

    if not window_results:
        raise ValueError("No window had enough data for every fold. Try a smaller test_days.")

    sorted_results = sorted(window_results, key=lambda r: r["models"][r["best_model"]]["log_loss"])
    logger.info("=" * 60)
    logger.info("SUMMARY (mean walk-forward log loss of best model per window)")
    for r in sorted_results:
        b = r["models"][r["best_model"]]
        logger.info(
            f"  {r['window']:8s} (~{r['train_rows']:5d} rows) log_loss={b['log_loss']:.4f} "
            f"±{b['log_loss_std']:.4f} acc={b['accuracy']:.4f} best_model={r['best_model']}"
        )
    optimal = sorted_results[0]
    logger.info(f"Optimal window: {optimal['window']}")

    result = {
        "test_period": [test_period[0][:10], test_period[1][:10]],
        "test_days": test_days,
        "n_folds": n_folds,
        "test_size": test_size,
        "symmetric": symmetric,
        "windows": window_results,
        "optimal_window": optimal["window"],
    }

    if record:
        from src.db.database import record_training_run
        best_ll = optimal["models"][optimal["best_model"]]["log_loss"]
        result["run_id"] = record_training_run(
            kind="window_search",
            best_model=optimal["best_model"],
            best_log_loss=best_ll,
            params={"test_days": test_days, "n_folds": n_folds, "symmetric": symmetric},
            result=result,
        )
    return result


if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Walk-forward training window search")
    ap.add_argument("--test-days", type=int, default=90)
    ap.add_argument("--folds", type=int, default=3)
    ap.add_argument("--no-record", action="store_true", help="Don't save to training_runs")
    args = ap.parse_args()
    print(json.dumps(run_window_search(args.test_days, args.folds, record=not args.no_record), indent=2))
