from src.utils.validators import validate_tournament_data
from src.utils.logger import get_parse_logger

logger = get_parse_logger()


def normalize_tournament(t):
    """
    Normalize tournament data from API response.

    Args:
        t: Raw tournament dictionary from API

    Returns:
        Normalized tournament dictionary or None if validation fails
    """
    # Validate tournament data first
    is_valid, errors = validate_tournament_data(t)
    if not is_valid:
        tournament_id = t.get("id") or t.get("pagename") or "unknown"
        logger.error(f"Invalid tournament data for {tournament_id}: {', '.join(errors)}")
        return None

    try:
        return {
            "tournament_id": t.get("id"),
            "name": t.get("name"),
            "pagename": t.get("pagename"),
            "startdate": t.get("startdate"),
            "enddate": t.get("enddate"),
            "tier": t.get("liquipediatier"),
            "prizepool": t.get("prizepool"),
            "location": t.get("location"),
            "type": t.get("type"),
            "game": t.get("game"),
        }

    except Exception as e:
        tournament_id = t.get("id") or t.get("pagename") or "unknown"
        logger.error(f"Error normalizing tournament {tournament_id}: {e}")
        return None
