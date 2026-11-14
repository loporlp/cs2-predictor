def _match(match_id, date="2024-01-01", winner=1):
    return {
        "match_id": match_id, "tournament_pagename": "T", "date": date, "bestof": 3,
        "team1_id": 1, "team1_name": "A", "team1_score": 2 if winner == 1 else 0,
        "team2_id": 2, "team2_name": "B", "team2_score": 0 if winner == 1 else 2,
        "winner_id": winner, "team1_win": int(winner == 1), "team2_win": int(winner == 2),
    }


def test_upsert_matches_is_idempotent(tmp_db):
    tmp_db.upsert_tournaments([{"pagename": "T", "name": "T", "tier": 1}])
    tmp_db.upsert_matches([_match("m1"), _match("m2")])
    tmp_db.upsert_matches([_match("m1", winner=2)])  # same id → replaced, not duplicated
    assert tmp_db.get_match_count() == 2
    df = tmp_db.load_matches_df()
    assert int(df.loc[df.match_id == "m1", "winner_id"].iloc[0]) == 2


def test_fetch_log_roundtrip(tmp_db):
    assert tmp_db.get_fetch_completed_pagenames() == set()
    tmp_db.mark_tournament_fetched("T")
    assert tmp_db.get_fetch_completed_pagenames() == {"T"}


def test_training_runs_roundtrip(tmp_db):
    r1 = tmp_db.record_training_run("train", "logistic_regression", 0.62, {"n_folds": 5}, {"x": 1})
    r2 = tmp_db.record_training_run("window_search", "random_forest", 0.61, {}, {"windows": []})
    assert r2 > r1
    runs = tmp_db.list_training_runs()
    assert [r["run_id"] for r in runs] == [r2, r1]
    assert [r["run_id"] for r in tmp_db.list_training_runs(kind="train")] == [r1]
    full = tmp_db.get_training_run(r1)
    assert full["params"] == {"n_folds": 5} and full["result"] == {"x": 1}
    assert tmp_db.get_training_run(9999) is None
