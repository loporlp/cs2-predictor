"""
Single source of truth for model feature columns, plus team-order symmetry helpers.

Liquipedia lists the higher seed first, so `team1` wins ~55% of matches and
still has a ~52.5% edge after controlling for Elo/form differences. At
prediction time the user picks the order, so the model must not reward
"being listed first". `swap_features` mirrors a feature matrix (team1 <-> team2)
so training data can be symmetrized and predictions averaged over both orders.
"""

import numpy as np
import pandas as pd

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

# Columns that are team1 - team2 differences: negate on swap.
NEGATE_ON_SWAP = ["elo_diff", "wr_diff_all", "form_diff", "streak_diff"]
# Columns that are team1's share vs team2: complement (1 - x) on swap.
COMPLEMENT_ON_SWAP = ["h2h_wr"]


def _swap_plan(cols):
    """Return (permutation, negate_mask, complement_mask) for a column list."""
    idx = {c: i for i, c in enumerate(cols)}
    perm = list(range(len(cols)))
    for c in cols:
        if c.startswith("team1_"):
            other = "team2_" + c[len("team1_"):]
            if other not in idx:
                raise ValueError(f"No team2 counterpart for {c}")
            perm[idx[c]], perm[idx[other]] = idx[other], idx[c]
    negate = np.array([c in NEGATE_ON_SWAP for c in cols])
    complement = np.array([c in COMPLEMENT_ON_SWAP for c in cols])
    return np.array(perm), negate, complement


_PERM, _NEGATE, _COMPLEMENT = _swap_plan(FEATURE_COLS)


def swap_features(X: np.ndarray) -> np.ndarray:
    """Mirror a (n, len(FEATURE_COLS)) matrix so team1 and team2 trade places."""
    X = np.asarray(X, dtype=float)
    Xs = X[:, _PERM].copy()
    Xs[:, _NEGATE] = -Xs[:, _NEGATE]
    Xs[:, _COMPLEMENT] = 1.0 - Xs[:, _COMPLEMENT]
    return Xs


def symmetrize(X: np.ndarray, y: np.ndarray):
    """Append the team-swapped copy of every row with the target flipped."""
    X = np.asarray(X, dtype=float)
    y = np.asarray(y)
    return np.vstack([X, swap_features(X)]), np.concatenate([y, 1 - y])


def feature_frame_to_xy(df: pd.DataFrame):
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")
    return df[FEATURE_COLS].to_numpy(dtype=float), df[TARGET].to_numpy(dtype=int)
