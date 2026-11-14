from collections import Counter

import numpy as np

from src.pickem import Team, create_round1_matchups, simulate_once


def _teams(n=16):
    return [Team(name=f"T{i}", seed=i) for i in range(1, n + 1)]


def test_round1_pairs_top_half_vs_bottom_half():
    pairs = create_round1_matchups(_teams())
    assert [(a.seed, b.seed) for a, b in pairs] == [(i, i + 8) for i in range(1, 9)]


def test_swiss_record_distribution():
    teams = _teams()
    probs = {}
    for a in teams:
        for b in teams:
            if a.name < b.name:
                probs[frozenset({a.name, b.name})] = {a.name: 0.5, b.name: 0.5}
    rng = np.random.default_rng(0)
    for _ in range(200):
        records = simulate_once(teams, probs, rng)
        assert len(records) == 16
        c = Counter(records.values())
        # 16-team Swiss (best of 3 wins/losses): 2 / 3 / 3 advance-by-record, 8 total advance
        assert c["3-0"] == 2 and c["0-3"] == 2
        assert c["3-0"] + c["3-1"] + c["3-2"] == 8
