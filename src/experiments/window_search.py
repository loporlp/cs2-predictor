"""
Training window search experiment.

Fixes a test period (last N days) and evaluates all models across different
training windows (3, 6, 9, 12, 18, 24 months, and all available data).
Metrics are directly comparable because the test set never changes.
"""

import os

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from src.model.evaluate import evaluate_model
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.experiments.window_search")

FEATURE_MATRIX_PATH = "data/processed/feature_matrix.csv"
FEATURE_COLS = [
    "team1_elo", "team2_elo", "elo_diff",
    "team1_wr_all", "team2_wr_all", "wr_diff_all",
    "team1_wr_20", "team2_wr_20", "team1_wr_50", "team2_wr_50",
    "team1_form", "team2_form", "form_diff",
    "team1_streak", "team2_streak", "streak_diff",
    "h2h_wr",
    "team1_tier_wr", "team2_tier_wr",
    "team1_days_since_last", "team2_days_since_last",
    "tier", "log_prizepool", "is_online", "is_offline", "is_hybrid",
]
TARGET = "team1_win"

# Training windows in months to test (None = all available data before test period)
WINDOWS_MONTHS = [3, 6, 9, 12, 18, 24, None]


def _get_models():
    return {
        "logistic_regression": LogisticRegression(max_iter=1000, random_state=42),
        "random_forest": RandomForestClassifier(
            n_estimators=200, max_depth=10, min_samples_leaf=20, random_state=42, n_jobs=-1
        ),
        "xgboost": XGBClassifier(
            n_estimators=200, max_depth=6, learning_rate=0.1,
            min_child_weight=20, random_state=42, eval_metric="logloss", verbosity=0,
        ),
        "gradient_boosting": GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.1,
            min_samples_leaf=20, random_state=42,
        ),
    }


def run_window_search(test_days=90, min_train_rows=500):
    """
    Run the training window search experiment.

    Args:
        test_days: Number of days at the end of the data to use as the fixed test set.
        min_train_rows: Skip windows with fewer training samples than this.

    Returns:
        dict with keys:
            - test_period: [start_date, end_date] strings
            - test_size: number of test rows
            - windows: list of per-window result dicts
    """
    if not os.path.exists(FEATURE_MATRIX_PATH):
        raise FileNotFoundError(
            f"Feature matrix not found at {FEATURE_MATRIX_PATH}. "
            "Run 'Retrain Model' first to build it."
        )

    logger.info("=" * 60)
    logger.info("TRAINING WINDOW SEARCH EXPERIMENT")
    logger.info("=" * 60)

    df = pd.read_csv(FEATURE_MATRIX_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    logger.info(f"Loaded feature matrix: {len(df)} rows ({df['date'].min().date()} → {df['date'].max().date()})")

    test_start = df["date"].max() - pd.Timedelta(days=test_days)
    df_test = df[df["date"] >= test_start]
    df_all_train = df[df["date"] < test_start]

    logger.info(f"Fixed test period: {test_start.date()} → {df['date'].max().date()} ({len(df_test)} rows)")
    logger.info(f"All data available for training: {len(df_all_train)} rows")
    logger.info("")

    if len(df_test) < 50:
        raise ValueError(
            f"Test set too small ({len(df_test)} rows). "
            "Try a larger test_days value or fetch more recent data."
        )

    X_test = df_test[FEATURE_COLS].values
    y_test = df_test[TARGET].values

    window_results = []

    for window_months in WINDOWS_MONTHS:
        if window_months is None:
            df_train = df_all_train
            label = "All data"
        else:
            train_start = test_start - pd.Timedelta(days=window_months * 30)
            df_train = df_all_train[df_all_train["date"] >= train_start]
            label = f"{window_months}m"

        if len(df_train) < min_train_rows:
            logger.info(f"Skipping window={label}: only {len(df_train)} train rows (min {min_train_rows})")
            continue

        train_start_actual = df_train["date"].min()
        logger.info(f"Window={label:8s}  train rows={len(df_train):5d}  from={train_start_actual.date()}")

        scaler = StandardScaler()
        X_train = df_train[FEATURE_COLS].values
        y_train = df_train[TARGET].values
        X_train_s = scaler.fit_transform(X_train)
        X_test_s = scaler.transform(X_test)

        model_metrics = {}
        for name, model in _get_models().items():
            if name == "logistic_regression":
                model.fit(X_train_s, y_train)
                y_proba = model.predict_proba(X_test_s)[:, 1]
                y_pred = model.predict(X_test_s)
            else:
                model.fit(X_train, y_train)
                y_proba = model.predict_proba(X_test)[:, 1]
                y_pred = model.predict(X_test)

            m = evaluate_model(y_test, y_pred, y_proba)
            model_metrics[name] = {
                "log_loss": round(m["log_loss"], 4),
                "accuracy": round(m["accuracy"], 4),
                "roc_auc": round(m["roc_auc"], 4),
            }

        # Pick best model for this window by log loss
        best_model = min(model_metrics, key=lambda k: model_metrics[k]["log_loss"])

        window_results.append({
            "window": label,
            "train_rows": len(df_train),
            "train_start": str(train_start_actual.date()),
            "models": model_metrics,
            "best_model": best_model,
        })

        best = model_metrics[best_model]
        logger.info(
            f"  best={best_model}  log_loss={best['log_loss']:.4f}  "
            f"acc={best['accuracy']:.4f}  auc={best['roc_auc']:.4f}"
        )

    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY (sorted by log loss of best model per window)")
    logger.info("=" * 60)
    sorted_results = sorted(window_results, key=lambda r: r["models"][r["best_model"]]["log_loss"])
    for r in sorted_results:
        best = r["models"][r["best_model"]]
        logger.info(
            f"  {r['window']:8s}  ({r['train_rows']:5d} rows)  "
            f"log_loss={best['log_loss']:.4f}  acc={best['accuracy']:.4f}  "
            f"auc={best['roc_auc']:.4f}  best_model={r['best_model']}"
        )

    optimal = sorted_results[0]
    logger.info(f"\nOptimal window: {optimal['window']} (train from {optimal['train_start']})")

    return {
        "test_period": [str(test_start.date()), str(df["date"].max().date())],
        "test_days": test_days,
        "test_size": len(df_test),
        "windows": window_results,
        "optimal_window": optimal["window"],
    }


if __name__ == "__main__":
    import json
    result = run_window_search()
    print(json.dumps(result, indent=2))
