"""
Model training pipeline.

1. Walk-forward backtest every model (see src/model/backtest.py) and pick the
   best by mean out-of-fold log loss — not by a single 80/20 split.
2. Choose a calibration method (none / Platt / isotonic) per model from rolling
   out-of-fold evaluation, and fit it on all out-of-fold predictions.
3. Refit every model on all rows and save it as a `Predictor` bundle
   (model + scaler + calibrator). Training is team-order symmetrized by default.
4. Record the run in the `training_runs` table.
"""

import argparse
import json
import os

import joblib

from src.model.backtest import load_feature_matrix, run_backtest, select_calibration
from src.model.evaluate import plot_calibration
from src.model.features import FEATURE_COLS, feature_frame_to_xy
from src.model.predictor import MODEL_NAMES, Calibrator, Predictor
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.model.train")

MODELS_DIR = "data/models"

DEFAULT_N_FOLDS = 5
DEFAULT_TEST_DAYS = 300  # total span covered by the backtest folds


def train_pipeline(cutoff_date=None, train_start_date=None, n_folds=DEFAULT_N_FOLDS,
                   test_days=DEFAULT_TEST_DAYS, symmetric=True, record=True,
                   models_dir=MODELS_DIR):
    """Backtest, select, calibrate, refit on all data, save, and record.

    Args:
        cutoff_date / train_start_date: recorded in metadata only (they are
            applied when the feature matrix is built).
        n_folds, test_days: walk-forward backtest configuration.
        symmetric: train on both team orders and average predictions over them.
        record: write the run to the training_runs table.
    Returns:
        dict of training metadata (also saved to training_metadata.json)
    """
    logger.info("=" * 60)
    logger.info("STARTING MODEL TRAINING PIPELINE")
    logger.info("=" * 60)

    df = load_feature_matrix()
    X, y = feature_frame_to_xy(df)
    logger.info(f"Loaded feature matrix: {len(df)} rows ({df['date'].min()} → {df['date'].max()})")
    logger.info(f"Order-symmetric training: {symmetric}")

    # 1. Walk-forward backtest
    bt = run_backtest(df, MODEL_NAMES, n_folds=n_folds, test_days=test_days, symmetric=symmetric)
    best_name = bt.best_model

    logger.info("-" * 60)
    logger.info(f"Backtest ({len(bt.folds)} folds, {bt.test_size} test rows) — mean metrics:")
    for name, m in sorted(bt.mean_metrics.items(), key=lambda kv: kv[1]["log_loss"]):
        logger.info(
            f"  {name:22s} log_loss={m['log_loss']:.4f} ±{m['log_loss_std']:.4f} "
            f"acc={m['accuracy']:.4f} auc={m['roc_auc']:.4f} brier={m['brier_score']:.4f}"
        )
    logger.info(f"Best model: {best_name}")

    # 2. Calibration selection per model
    calibration = {}
    for name in MODEL_NAMES:
        calibration[name] = select_calibration(bt.oof_proba[name], bt.oof_y, bt.oof_fold)
        cal = calibration[name]
        if cal.get("log_loss"):
            ll = ", ".join(f"{k}={v:.4f}" for k, v in cal["log_loss"].items())
            logger.info(f"  calibration {name:22s} → {cal['method']:8s} ({ll})")

    # 3. Refit on all data and save
    os.makedirs(models_dir, exist_ok=True)
    predictors = {}
    for name in MODEL_NAMES:
        logger.info(f"Refitting {name} on all {len(X)} rows…")
        pred = Predictor.fit(name, X, y, symmetric=symmetric)
        method = calibration[name]["method"]
        if method != "none":
            pred.calibrator = Calibrator(method).fit(bt.oof_proba[name], bt.oof_y)
        predictors[name] = pred
        joblib.dump(pred, os.path.join(models_dir, f"model_{name}.joblib"))
    joblib.dump(predictors[best_name], os.path.join(models_dir, "best_model.joblib"))

    # Stale artifact from the pre-Predictor format (scaler now lives in each bundle)
    legacy_scaler = os.path.join(models_dir, "scaler.joblib")
    if os.path.exists(legacy_scaler):
        os.remove(legacy_scaler)

    importances = {name: p.feature_importances(FEATURE_COLS) for name, p in predictors.items()}
    with open(os.path.join(models_dir, "feature_importances.json"), "w") as f:
        json.dump(importances, f, indent=2)

    # Calibration plot on out-of-fold predictions for the best model
    curves = {f"{best_name} (uncalibrated)": bt.oof_proba[best_name]}
    best_method = calibration[best_name]["method"]
    if best_method != "none":
        c = predictors[best_name].calibrator
        p = bt.oof_proba[best_name]
        curves[f"{best_name} + {best_method} (in-sample)"] = 0.5 * (c.transform(p) + 1 - c.transform(1 - p))
    plot_calibration(bt.oof_y, curves, best_name, models_dir)

    metadata = {
        "best_model": best_name,
        "cutoff_date": cutoff_date,
        "train_start_date": train_start_date,
        "symmetric": symmetric,
        "feature_columns": FEATURE_COLS,
        "selection": "walk_forward_mean_log_loss",
        # Final models are fit on every row; test_* describe the backtest folds.
        "train_size": int(len(X)),
        "test_size": bt.test_size,
        "train_period": [str(df["date"].iloc[0]), str(df["date"].iloc[-1])],
        "test_period": bt.test_period,
        "results": bt.mean_metrics,
        "calibration": {
            name: {"method": c["method"], "log_loss": c.get("log_loss", {})}
            for name, c in calibration.items()
        },
        "backtest": {"n_folds": n_folds, "test_days": test_days, "folds": bt.folds},
    }

    if record:
        from src.db.database import record_training_run
        metadata["run_id"] = record_training_run(
            kind="train",
            best_model=best_name,
            best_log_loss=bt.mean_metrics[best_name]["log_loss"],
            params={"cutoff_date": cutoff_date, "train_start_date": train_start_date,
                    "n_folds": n_folds, "test_days": test_days, "symmetric": symmetric},
            result=metadata,
        )
        logger.info(f"Recorded training run #{metadata['run_id']}")

    with open(os.path.join(models_dir, "training_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    logger.info("=" * 60)
    logger.info("TRAINING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Models saved to: {models_dir}/  (best: {best_name}, calibration: {best_method})")
    return metadata


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backtest, select, calibrate and train models")
    ap.add_argument("--folds", type=int, default=DEFAULT_N_FOLDS, help="walk-forward folds (default 5)")
    ap.add_argument("--test-days", type=int, default=DEFAULT_TEST_DAYS,
                    help="total days covered by the backtest folds (default 300)")
    ap.add_argument("--no-symmetric", action="store_true",
                    help="train on raw Liquipedia team order (not recommended; for comparison)")
    ap.add_argument("--no-record", action="store_true", help="don't save to training_runs")
    args = ap.parse_args()
    train_pipeline(n_folds=args.folds, test_days=args.test_days,
                   symmetric=not args.no_symmetric, record=not args.no_record)
