import numpy as np

from src.features.build_features import extract_features, fill_feature_defaults
from src.features.elo import EloRatingSystem
from src.features.team_stats import TeamStatsTracker
from src.model.features import FEATURE_COLS
from src.predict import build_feature_vector

import pandas as pd


def _trackers():
    elo, stats = EloRatingSystem(), TeamStatsTracker()
    # B never beats A → A's h2h vs B is 1.0, B's is 0.0
    for i in range(6):
        d = f"2024-01-{i + 1:02d}"
        elo.update("A", "B", 1)
        stats.record_match("A", "B", 1, d, tier=1)
    return elo, stats


def test_elo_update_moves_ratings_correctly():
    elo = EloRatingSystem()
    elo.update("A", "B", 1)
    assert elo.get_rating("A") > 1500 > elo.get_rating("B")
    # Equal K for new teams → zero-sum
    assert elo.get_rating("A") + elo.get_rating("B") == 3000


def test_prediction_features_match_training_features():
    """build_feature_vector must reproduce training-time extraction exactly (no skew)."""
    elo, stats = _trackers()
    date = "2024-01-06"
    train_row = fill_feature_defaults(pd.DataFrame([
        extract_features(elo, stats, "B", "A", date, 1, 10000, "Offline")
    ]))[FEATURE_COLS].to_numpy(float)
    serve_row = build_feature_vector(elo, stats, "B", "A", 1, 10000, "Offline", date_str=date)
    np.testing.assert_allclose(serve_row, train_row)
    # A real 0% head-to-head stays 0, not the 0.5 "unknown" default
    assert serve_row[0, FEATURE_COLS.index("h2h_wr")] == 0.0


def test_unknown_teams_get_defaults():
    elo, stats = _trackers()
    X = build_feature_vector(elo, stats, "New1", "New2", 2, 0, "Online", date_str="2024-02-01")
    v = dict(zip(FEATURE_COLS, X[0]))
    assert v["elo_diff"] == 0 and v["h2h_wr"] == 0.5 and v["team1_days_since_last"] == 30
