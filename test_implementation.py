#!/usr/bin/env python3
"""
Quick verification test for the data retrieval improvements.

This script tests that all components work together without making actual API calls.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))


def test_imports():
    """Test that all modules import successfully."""
    print("Testing imports...")

    try:
        # Utility modules
        from src.utils.exceptions import (
            DataFetchException, NetworkException, APIException,
            RateLimitException, DataValidationException, PaginationException
        )
        from src.utils.logger import get_fetch_logger, get_parse_logger, get_pipeline_logger
        from src.utils.validators import validate_tournament_data, validate_match_data, safe_get_opponent
        from src.utils.retry_handler import retry_with_backoff
        from src.utils.rate_limiter_v2 import RateLimiter

        # Config
        from src.config import (
            CONNECT_TIMEOUT, READ_TIMEOUT, MAX_RETRIES,
            MAX_PAGINATION_ITERATIONS, rate_limiter
        )

        # Fetch modules
        from src.fetch.tournaments import fetch_all_cs2_tournaments
        from src.fetch.matches import fetch_all_matches_for_tournament

        # Parse modules
        from src.parse.tournaments import normalize_tournament
        from src.parse.matches import normalize_match

        # Pipeline modules
        from src.pipelines.build_tournaments import build_tournament_table
        from src.pipelines.build_matches import build_match_table

        print("✓ All imports successful")
        return True

    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False


def test_validators():
    """Test validator functions."""
    print("\nTesting validators...")

    from src.utils.validators import (
        validate_tournament_data, validate_match_data,
        safe_get_opponent, is_valid_date
    )

    # Test tournament validation
    valid_tournament = {
        'pagename': 'Test_Tournament',
        'startdate': '2024-01-01',
        'enddate': '2024-01-05',
        'prizepool': 10000
    }
    is_valid, errors = validate_tournament_data(valid_tournament)
    assert is_valid, f"Valid tournament should pass: {errors}"

    invalid_tournament = {
        'startdate': 'invalid-date',
        'prizepool': 'not-a-number'
    }
    is_valid, errors = validate_tournament_data(invalid_tournament)
    assert not is_valid, "Invalid tournament should fail"
    assert len(errors) > 0, "Should have validation errors"

    # Test match validation
    valid_match = {
        'match_id': '123',
        'match2opponents': [
            {'id': '1', 'name': 'Team1', 'score': '2'},
            {'id': '2', 'name': 'Team2', 'score': '1'}
        ],
        'winner': '1',
        'date': '2024-01-01'
    }
    is_valid, errors = validate_match_data(valid_match)
    assert is_valid, f"Valid match should pass: {errors}"

    # Test safe_get_opponent
    opponents = [{'name': 'Team1'}, {'name': 'Team2'}]
    team1 = safe_get_opponent(opponents, 0)
    assert team1 == {'name': 'Team1'}, "Should get first opponent"

    team3 = safe_get_opponent(opponents, 5)
    assert team3 == {}, "Out of bounds should return empty dict"

    # Test date validation
    assert is_valid_date('2024-01-01'), "Valid date should pass"
    assert not is_valid_date('invalid'), "Invalid date should fail"

    print("✓ All validator tests passed")
    return True


def test_rate_limiter():
    """Test rate limiter functionality."""
    print("\nTesting rate limiter...")

    import time
    from src.utils.rate_limiter_v2 import RateLimiter

    # Test distributed strategy with small values
    limiter = RateLimiter(max_requests=3, window_seconds=3, strategy='distributed')

    start = time.time()
    for i in range(3):
        limiter.wait_if_needed()

    elapsed = time.time() - start
    expected_min = 1.0  # Should take at least 1 second with distributed strategy
    assert elapsed >= expected_min, f"Distributed strategy should space requests (took {elapsed:.2f}s)"

    stats = limiter.get_stats()
    assert stats['requests_in_window'] == 3, "Should track 3 requests"
    assert stats['strategy'] == 'distributed', "Should use distributed strategy"

    print(f"✓ Rate limiter test passed (took {elapsed:.2f}s)")
    return True


def test_exceptions():
    """Test exception hierarchy."""
    print("\nTesting exceptions...")

    from src.utils.exceptions import (
        DataFetchException, NetworkException, APIException
    )

    # Test basic exception
    exc = DataFetchException(
        "Test error",
        url="https://example.com",
        params={'key': 'value'},
        status_code=500
    )
    assert "Test error" in str(exc)
    assert "https://example.com" in str(exc)
    assert exc.status_code == 500

    # Test inheritance
    net_exc = NetworkException("Network error")
    assert isinstance(net_exc, DataFetchException)

    api_exc = APIException("API error")
    assert isinstance(api_exc, DataFetchException)

    print("✓ Exception tests passed")
    return True


def test_parse_functions():
    """Test parse functions with mock data."""
    print("\nTesting parse functions...")

    from src.parse.tournaments import normalize_tournament
    from src.parse.matches import normalize_match

    # Test tournament parsing
    tournament = {
        'id': '123',
        'name': 'Test Tournament',
        'pagename': 'Test_Tournament',
        'startdate': '2024-01-01',
        'enddate': '2024-01-05',
        'liquipediatier': 'S-Tier',
        'prizepool': 10000
    }
    result = normalize_tournament(tournament)
    assert result is not None, "Valid tournament should parse"
    assert result['tournament_id'] == '123'
    assert result['name'] == 'Test Tournament'

    # Test match parsing
    match = {
        'id': 'match123',
        'parent': 'Test_Tournament',
        'date': '2024-01-01',
        'match2opponents': [
            {'id': '1', 'name': 'Team1', 'score': '2'},
            {'id': '2', 'name': 'Team2', 'score': '1'}
        ],
        'winner': '1',
        'extradata': {'bestof': '3'}
    }
    result = normalize_match(match)
    assert result is not None, "Valid match should parse"
    assert result['match_id'] == 'match123'
    assert result['team1_name'] == 'Team1'
    assert result['team2_name'] == 'Team2'
    assert result['team1_win'] == 1
    assert result['team2_win'] == 0

    # Test invalid match (missing opponents)
    invalid_match = {
        'id': 'bad_match',
        'match2opponents': []  # Empty opponents
    }
    result = normalize_match(invalid_match)
    assert result is None, "Invalid match should return None"

    print("✓ Parse function tests passed")
    return True


def main():
    """Run all tests."""
    print("=" * 80)
    print("CS2 Predictor - Data Retrieval Improvements Verification")
    print("=" * 80)

    tests = [
        ("Imports", test_imports),
        ("Validators", test_validators),
        ("Rate Limiter", test_rate_limiter),
        ("Exceptions", test_exceptions),
        ("Parse Functions", test_parse_functions),
    ]

    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"✗ {name} test failed with exception: {e}")
            results.append((name, False))

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {name}")

    print("=" * 80)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 80)

    if passed == total:
        print("\n🎉 All tests passed! The implementation is ready to use.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
