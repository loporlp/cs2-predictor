import time

from src.utils.exceptions import APIException, DataFetchException, NetworkException
from src.utils.rate_limiter_v2 import RateLimiter
from src.utils.validators import (
    is_valid_date, safe_get_opponent, validate_match_data, validate_tournament_data,
)


def test_validate_tournament():
    ok, errors = validate_tournament_data({
        "pagename": "T", "startdate": "2024-01-01", "enddate": "2024-01-05", "prizepool": 10000,
    })
    assert ok, errors
    ok, errors = validate_tournament_data({"startdate": "invalid-date", "prizepool": "x"})
    assert not ok and errors


def test_validate_match():
    ok, errors = validate_match_data({
        "match_id": "123",
        "match2opponents": [{"id": "1", "name": "A", "score": "2"}, {"id": "2", "name": "B", "score": "1"}],
        "winner": "1", "date": "2024-01-01",
    })
    assert ok, errors


def test_safe_get_opponent():
    opps = [{"name": "A"}, {"name": "B"}]
    assert safe_get_opponent(opps, 0) == {"name": "A"}
    assert safe_get_opponent(opps, 5) == {}


def test_is_valid_date():
    assert is_valid_date("2024-01-01")
    assert not is_valid_date("invalid")


def test_exception_hierarchy():
    exc = DataFetchException("Test error", url="https://example.com", params={"k": "v"}, status_code=500)
    assert "Test error" in str(exc) and "https://example.com" in str(exc)
    assert exc.status_code == 500
    assert isinstance(NetworkException("x"), DataFetchException)
    assert isinstance(APIException("x"), DataFetchException)


def test_rate_limiter_distributed_spacing():
    limiter = RateLimiter(max_requests=3, window_seconds=3, strategy="distributed")
    start = time.monotonic()
    for _ in range(3):
        limiter.wait_if_needed()
    assert time.monotonic() - start >= 1.0  # monotonic: same clock as the limiter
    stats = limiter.get_stats()
    assert stats["requests_in_window"] == 3
    assert stats["strategy"] == "distributed"
