"""
Data validation utilities for CS2 Predictor.

Provides validators for tournament and match data with detailed error reporting.
"""

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime


def safe_get_opponent(opponents: List[Dict[str, Any]], index: int) -> Dict[str, Any]:
    """
    Safely access opponent from list with bounds checking.

    Args:
        opponents: List of opponent dictionaries
        index: Index to access

    Returns:
        Opponent dictionary or empty dict if index out of bounds
    """
    if not opponents or not isinstance(opponents, list):
        return {}
    if index < 0 or index >= len(opponents):
        return {}
    return opponents[index] if opponents[index] is not None else {}


def validate_tournament_data(tournament: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate tournament data structure and content.

    Args:
        tournament: Tournament dictionary from API

    Returns:
        Tuple of (is_valid, list_of_error_messages)
    """
    errors = []

    # Check required fields
    if not tournament:
        errors.append("Tournament data is empty or None")
        return False, errors

    # Check for tournament identifier
    if not tournament.get("pagename") and not tournament.get("tournament"):
        errors.append("Missing tournament identifier (pagename or tournament)")

    # Validate dates if present
    date_fields = ["startdate", "enddate"]
    for field in date_fields:
        date_str = tournament.get(field)
        if date_str and date_str != "":
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except (ValueError, TypeError):
                errors.append(f"Invalid date format for {field}: {date_str}")

    # Check prize pool if present
    prize_pool = tournament.get("prizepool")
    if prize_pool is not None and prize_pool != "":
        try:
            float(str(prize_pool))
        except (ValueError, TypeError):
            errors.append(f"Invalid prizepool value: {prize_pool}")

    return len(errors) == 0, errors


def validate_match_data(match: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate match data structure and content.

    Args:
        match: Match dictionary from API

    Returns:
        Tuple of (is_valid, list_of_error_messages)
    """
    errors = []

    # Check required fields
    if not match:
        errors.append("Match data is empty or None")
        return False, errors

    # Check for match identifier
    if not match.get("match_id") and not match.get("match2id") and not match.get("id"):
        errors.append("Missing match identifier (id, match_id, or match2id)")

    # Validate opponents
    opponents = match.get("match2opponents", [])
    if not isinstance(opponents, list):
        errors.append(f"match2opponents is not a list: {type(opponents)}")
    elif len(opponents) < 2:
        errors.append(f"Insufficient opponents: expected 2, got {len(opponents)}")
    else:
        # Validate each opponent has required fields
        for i, opponent in enumerate(opponents[:2]):
            if not isinstance(opponent, dict):
                errors.append(f"Opponent {i} is not a dictionary: {type(opponent)}")
                continue

            if not opponent.get("name") and not opponent.get("template"):
                errors.append(f"Opponent {i} missing name/template")

    # Validate winner consistency
    winner = match.get("winner")
    if winner is not None:
        try:
            winner_int = int(winner)
            if winner_int not in [0, 1, 2]:
                errors.append(f"Invalid winner value: {winner} (expected 0, 1, or 2)")
        except (ValueError, TypeError):
            errors.append(f"Winner is not a valid integer: {winner}")

    # Validate date if present
    date_str = match.get("date")
    if date_str and date_str != "":
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            # Try alternative format with time
            try:
                datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                errors.append(f"Invalid date format: {date_str}")

    # Validate scores if present
    for i in [1, 2]:
        score_key = f"match2opponents.{i-1}.score"
        # This is a nested check - we'd need to look at the actual opponent
        if len(opponents) >= i:
            opponent = opponents[i-1]
            if isinstance(opponent, dict):
                score = opponent.get("score")
                if score is not None and score != "":
                    try:
                        int(score)
                    except (ValueError, TypeError):
                        errors.append(f"Invalid score for team{i}: {score}")

    return len(errors) == 0, errors


def is_valid_date(date_str: str, format: str = "%Y-%m-%d") -> bool:
    """
    Check if a date string is valid.

    Args:
        date_str: Date string to validate
        format: Expected date format

    Returns:
        True if valid, False otherwise
    """
    if not date_str or date_str == "":
        return False

    try:
        datetime.strptime(date_str, format)
        return True
    except (ValueError, TypeError):
        return False
