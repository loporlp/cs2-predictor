import json
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from src.model.evaluate import evaluate_model, plot_calibration, print_evaluation_report
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.model.train")

FEATURE_MATRIX_PATH = "data/processed/feature_matrix.csv"
MODELS_DIR = "data/models"

# Features used for training (excludes target and metadata columns)
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


def get_models():
    """Return dict of model name -> model instance."""
    return {
        "logistic_regression": LogisticRegression(max_iter=1000, random_state=42),
        "random_forest": RandomForestClassifier(
            n_estimators=200, max_depth=10, min_samples_leaf=20, random_state=42, n_jobs=-1
        ),
        "xgboost": XGBClassifier(
            n_estimators=200, max_depth=6, learning_rate=0.1,
            min_child_weight=20, random_state=42, eval_metric="logloss",
            verbosity=0,
        ),
        "gradient_boosting": GradientBoostingClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.1,
            min_samples_leaf=20, random_state=42,
        ),
    }


def train_pipeline():
    """Full training pipeline: load, split, train, evaluate, save."""
    logger.info("=" * 60)
    logger.info("STARTING MODEL TRAINING PIPELINE")
    logger.info("=" * 60)

    # Load feature matrix
    df = pd.read_csv(FEATURE_MATRIX_PATH)
    logger.info(f"Loaded feature matrix: {len(df)} rows, {len(df.columns)} columns")

    # Verify all feature columns exist
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    X = df[FEATURE_COLS].values
    y = df[TARGET].values

    # Time-based split (80/20 chronologically)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    logger.info(f"Time-based split: {len(X_train)} train, {len(X_test)} test")
    logger.info(f"Train period: {df['date'].iloc[0]} to {df['date'].iloc[split_idx - 1]}")
    logger.info(f"Test period: {df['date'].iloc[split_idx]} to {df['date'].iloc[-1]}")

    # Fit scaler on train data (for logistic regression)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    os.makedirs(MODELS_DIR, exist_ok=True)

    # Train and evaluate all models
    models = get_models()
    results = {}

    logger.info("")
    logger.info("Model evaluation on test set:")
    logger.info("-" * 100)

    for name, model in models.items():
        # Logistic regression uses scaled data; tree models use raw
        if name == "logistic_regression":
            model.fit(X_train_scaled, y_train)
            y_pred = model.predict(X_test_scaled)
            y_proba = model.predict_proba(X_test_scaled)[:, 1]
        else:
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            y_proba = model.predict_proba(X_test)[:, 1]

        metrics = evaluate_model(y_test, y_pred, y_proba)
        results[name] = {"model": model, "metrics": metrics, "y_proba": y_proba}
        print_evaluation_report(name, metrics)

        # Save individual model
        model_path = os.path.join(MODELS_DIR, f"model_{name}.joblib")
        joblib.dump(model, model_path)

    # Select best model by log loss
    best_name = min(results, key=lambda k: results[k]["metrics"]["log_loss"])
    best_metrics = results[best_name]["metrics"]
    logger.info("-" * 100)
    logger.info(f"Best model: {best_name} (log_loss={best_metrics['log_loss']:.4f})")

    # Save best model
    best_model_path = os.path.join(MODELS_DIR, "best_model.joblib")
    joblib.dump(results[best_name]["model"], best_model_path)
    logger.info(f"Saved best model to {best_model_path}")

    # Save scaler
    scaler_path = os.path.join(MODELS_DIR, "scaler.joblib")
    joblib.dump(scaler, scaler_path)

    # Save training metadata
    metadata = {
        "best_model": best_name,
        "feature_columns": FEATURE_COLS,
        "train_size": len(X_train),
        "test_size": len(X_test),
        "train_period": [str(df["date"].iloc[0]), str(df["date"].iloc[split_idx - 1])],
        "test_period": [str(df["date"].iloc[split_idx]), str(df["date"].iloc[-1])],
        "results": {
            name: res["metrics"] for name, res in results.items()
        },
    }
    metadata_path = os.path.join(MODELS_DIR, "training_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    # Save feature importances (for tree-based models)
    importances = {}
    for name in ["random_forest", "xgboost", "gradient_boosting"]:
        model = results[name]["model"]
        imp = model.feature_importances_
        importances[name] = {
            FEATURE_COLS[i]: float(imp[i]) for i in range(len(FEATURE_COLS))
        }
    # Logistic regression coefficients
    lr_model = results["logistic_regression"]["model"]
    importances["logistic_regression"] = {
        FEATURE_COLS[i]: float(abs(lr_model.coef_[0][i])) for i in range(len(FEATURE_COLS))
    }

    imp_path = os.path.join(MODELS_DIR, "feature_importances.json")
    with open(imp_path, "w") as f:
        json.dump(importances, f, indent=2)

    # Calibration plot for best model
    plot_calibration(
        y_test, results[best_name]["y_proba"], best_name, MODELS_DIR
    )

    logger.info("=" * 60)
    logger.info("TRAINING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Models saved to: {MODELS_DIR}/")
    logger.info(f"Best model: {best_name}")
    for name, res in sorted(results.items(), key=lambda x: x[1]["metrics"]["log_loss"]):
        m = res["metrics"]
        logger.info(f"  {name}: log_loss={m['log_loss']:.4f}, accuracy={m['accuracy']:.4f}, auc={m['roc_auc']:.4f}")


if __name__ == "__main__":
    train_pipeline()
