from src.parse.matches import normalize_match
from src.parse.tournaments import normalize_tournament


def _match(**overrides):
    m = {
        "match2id": "12345",
        "parent": "Event/2024",
        "date": "2024-01-15",
        "match2opponents": [
            {"id": "1", "name": "Team A", "score": "2"},
            {"id": "2", "name": "Team B", "score": "1"},
        ],
        "winner": "1",
        "bestof": 3,
    }
    m.update(overrides)
    return m


def test_normalize_match_basic():
    r = normalize_match(_match())
    assert r["match_id"] == "12345"
    assert r["team1_name"] == "Team A" and r["team2_name"] == "Team B"
    assert (r["team1_win"], r["team2_win"]) == (1, 0)


def test_normalize_match_team2_wins():
    r = normalize_match(_match(winner="2"))
    assert (r["team1_win"], r["team2_win"]) == (0, 1)


def test_normalize_match_preserves_api_order():
    """Team order comes straight from match2opponents — never re-sorted."""
    r = normalize_match(_match(match2opponents=[
        {"id": "1", "name": "Zeta", "score": "0"},
        {"id": "2", "name": "Alpha", "score": "2"},
    ], winner="2"))
    assert r["team1_name"] == "Zeta"


def test_match_id_falls_back_to_id():
    m = _match()
    del m["match2id"]
    m["id"] = "67890"
    assert normalize_match(m)["match_id"] == "67890"


def test_match_without_id_returns_none():
    m = _match()
    del m["match2id"]
    assert normalize_match(m) is None


def test_match_without_opponents_returns_none():
    assert normalize_match({"id": "bad", "match2opponents": []}) is None


def test_normalize_tournament():
    r = normalize_tournament({
        "id": "123", "name": "Test Tournament", "pagename": "Test_Tournament",
        "startdate": "2024-01-01", "enddate": "2024-01-05",
        "liquipediatier": "S-Tier", "prizepool": 10000,
    })
    assert r["tournament_id"] == "123"
    assert r["name"] == "Test Tournament"
