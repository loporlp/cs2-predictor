import numpy as np
import pandas as pd
import pytest

from src.model.features import FEATURE_COLS


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Point src.db.database at a throwaway SQLite file."""
    from src.db import database

    path = tmp_path / "test.db"
    monkeypatch.setattr(database, "DB_PATH", str(path))
    database.initialize_db()
    return database


@pytest.fixture
def synthetic_features():
    """A dated feature matrix with a learnable signal (elo_diff) and a team1 bias."""
    rng = np.random.default_rng(0)
    n = 3000
    df = pd.DataFrame(rng.normal(size=(n, len(FEATURE_COLS))), columns=FEATURE_COLS)
    df["team1_elo"] = 1500 + rng.normal(0, 100, n)
    df["team2_elo"] = 1500 + rng.normal(0, 100, n)
    df["elo_diff"] = df["team1_elo"] - df["team2_elo"]
    df["h2h_wr"] = rng.uniform(0, 1, n)
    logit = df["elo_diff"] / 100 + 0.2  # +0.2 = order bias like Liquipedia's
    df["team1_win"] = (rng.uniform(size=n) < 1 / (1 + np.exp(-logit))).astype(int)
    df["date"] = pd.date_range("2024-01-01", periods=n, freq="6h")
    return df
