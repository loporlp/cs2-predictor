from src.utils.validators import safe_get_opponent, validate_match_data
from src.utils.logger import get_parse_logger

logger = get_parse_logger()


def normalize_match(m):
    """
    Normalize match data from API response.

    Args:
        m: Raw match dictionary from API

    Returns:
        Normalized match dictionary or None if validation fails
    """
    # Validate match data first
    is_valid, errors = validate_match_data(m)
    if not is_valid:
        match_id = m.get("id") or m.get("match2id") or "unknown"
        logger.error(f"Invalid match data for match {match_id}: {', '.join(errors)}")
        return None

    try:
        # Safely extract opponent data
        opponents = m.get("match2opponents", [])
        team1 = safe_get_opponent(opponents, 0)
        team2 = safe_get_opponent(opponents, 1)
        winner_id = m.get("winner")

        # Extract match ID with fallback
        match_id = m.get("match2id") or m.get("id") or m.get("match_id")

        # Critical validation: match_id must not be None
        if match_id is None:
            logger.error(f"Match has no valid ID field. Available keys: {list(m.keys())}")
            return None

        return {
            "match_id": match_id,
            "tournament_pagename": m.get("parent"),
            "date": m.get("date"),
            "bestof": m.get("extradata", {}).get("bestof") if m.get("extradata") else None,
            "team1_id": team1.get("id"),
            "team1_name": team1.get("name"),
            "team1_score": team1.get("score"),
            "team2_id": team2.get("id"),
            "team2_name": team2.get("name"),
            "team2_score": team2.get("score"),
            "winner_id": winner_id,
            "team1_win": 1 if str(team1.get("id")) == str(winner_id) else 0,
            "team2_win": 1 if str(team2.get("id")) == str(winner_id) else 0,
        }

    except Exception as e:
        match_id = m.get("id") or m.get("match2id") or "unknown"
        logger.error(f"Error normalizing match {match_id}: {e}")
        return None