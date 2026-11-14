import os

import numpy as np
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

from src.utils.logger import get_logger

logger = get_logger("cs2predictor.model.evaluate")


def evaluate_model(y_true, y_pred, y_pred_proba):
    """Compute evaluation metrics.

    Returns:
        dict with accuracy, log_loss, roc_auc, brier_score
    """
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "log_loss": log_loss(y_true, y_pred_proba),
        "roc_auc": roc_auc_score(y_true, y_pred_proba),
        "brier_score": brier_score_loss(y_true, y_pred_proba),
    }


def evaluate_probabilities(y_true, y_pred_proba):
    """Metrics from P(class 1) alone (predicted class = proba > 0.5)."""
    y_pred_proba = np.clip(np.asarray(y_pred_proba, dtype=float), 1e-6, 1 - 1e-6)
    return {
        "accuracy": float(accuracy_score(y_true, (y_pred_proba > 0.5).astype(int))),
        "log_loss": float(log_loss(y_true, y_pred_proba, labels=[0, 1])),
        "roc_auc": float(roc_auc_score(y_true, y_pred_proba)),
        "brier_score": float(brier_score_loss(y_true, y_pred_proba)),
    }


def print_evaluation_report(model_name, metrics):
    """Print formatted evaluation report to console."""
    logger.info(f"  {model_name:25s} | Acc: {metrics['accuracy']:.4f} | LogLoss: {metrics['log_loss']:.4f} | AUC: {metrics['roc_auc']:.4f} | Brier: {metrics['brier_score']:.4f}")


def plot_calibration(y_true, y_pred_proba, model_name, output_dir):
    """Generate and save a calibration curve plot.

    `y_pred_proba` may be a single array or a dict of {label: array} to overlay
    several curves (e.g. uncalibrated vs. calibrated).
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.calibration import calibration_curve

    curves = y_pred_proba if isinstance(y_pred_proba, dict) else {model_name: y_pred_proba}

    fig, ax = plt.subplots(figsize=(8, 6))
    for label, proba in curves.items():
        fraction_of_positives, mean_predicted_value = calibration_curve(
            y_true, proba, n_bins=10, strategy="uniform"
        )
        ax.plot(mean_predicted_value, fraction_of_positives, "s-", label=label)
    ax.plot([0, 1], [0, 1], "k--", label="Perfectly calibrated")
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Fraction of positives")
    ax.set_title(f"Calibration Curve - {model_name}")
    ax.legend()
    ax.grid(True, alpha=0.3)

    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, "calibration_curve.png")
    fig.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Saved calibration plot to {filepath}")
