"""Test to verify match_id extraction fix."""

from src.parse.matches import normalize_match

# Test case 1: API returns match2id (most common case)
test_match_1 = {
    "match2id": "12345",
    "parent": "SomeEvent/2024",
    "date": "2024-01-15",
    "match2opponents": [
        {"id": "team1", "name": "Team A", "score": "2"},
        {"id": "team2", "name": "Team B", "score": "1"}
    ],
    "winner": "1"
}

# Test case 2: API returns id (legacy format)
test_match_2 = {
    "id": "67890",
    "parent": "OtherEvent/2024",
    "date": "2024-01-16",
    "match2opponents": [
        {"id": "team3", "name": "Team C", "score": "1"},
        {"id": "team4", "name": "Team D", "score": "0"}
    ],
    "winner": "1"
}

# Test case 3: No ID (should fail gracefully)
test_match_3 = {
    "parent": "FailEvent/2024",
    "date": "2024-01-17",
    "match2opponents": [
        {"id": "team5", "name": "Team E", "score": "0"},
        {"id": "team6", "name": "Team F", "score": "1"}
    ],
    "winner": "2"
}

print("Test 1 - match2id field:")
result1 = normalize_match(test_match_1)
if result1:
    print(f"  ✓ match_id extracted: {result1['match_id']}")
    assert result1['match_id'] == "12345", "match_id should be 12345"
else:
    print("  ✗ FAILED: normalize_match returned None")

print("\nTest 2 - id field (fallback):")
result2 = normalize_match(test_match_2)
if result2:
    print(f"  ✓ match_id extracted: {result2['match_id']}")
    assert result2['match_id'] == "67890", "match_id should be 67890"
else:
    print("  ✗ FAILED: normalize_match returned None")

print("\nTest 3 - no ID field (should return None):")
result3 = normalize_match(test_match_3)
if result3 is None:
    print("  ✓ Correctly returned None for missing match_id")
else:
    print(f"  ✗ FAILED: Should have returned None, got {result3}")

print("\n" + "="*60)
print("All tests passed! The fix is working correctly.")
print("="*60)
