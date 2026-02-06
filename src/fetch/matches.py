import requests
import json
from src.config import BASE_URLS, HEADERS, CONNECT_TIMEOUT, READ_TIMEOUT, rate_limiter
from src.utils.retry_handler import retry_with_backoff
from src.utils.logger import get_fetch_logger
from src.utils.exceptions import APIException, NetworkException

logger = get_fetch_logger()


@retry_with_backoff(max_retries=3, base_delay=1.0)
def fetch_matches(tournament_pagename, offset=0, limit=1000):
    """
    Fetch a single page of matches for a given tournament.

    Args:
        tournament_pagename: Tournament identifier
        offset: Pagination offset
        limit: Maximum results per page

    Returns:
        API response dictionary

    Raises:
        APIException: For API-specific errors
        NetworkException: For network-related errors
    """
    # Enforce rate limit before calling the API
    rate_limiter.wait_if_needed()

    conditions = f"[[parent::{tournament_pagename}]]"

    params = {
        "wiki": "counterstrike",
        "limit": limit,
        "offset": offset,
        "conditions": conditions,
        "order": "date ASC",
        "includehidden": "true",
    }

    try:
        logger.debug(
            f"Fetching matches for {tournament_pagename}: offset={offset}, limit={limit}"
        )
        r = requests.get(
            BASE_URLS["match"],
            headers=HEADERS,
            params=params,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        r.raise_for_status()
        return r.json()

    except requests.exceptions.Timeout as e:
        logger.error(f"Request timeout while fetching matches for {tournament_pagename}: {e}")
        raise NetworkException(
            f"Request timed out for tournament {tournament_pagename}",
            url=BASE_URLS["match"],
            params=params
        ) from e

    except requests.exceptions.HTTPError as e:
        # Log but don't raise for individual tournament failures
        # Let retry handler deal with retryable errors first
        logger.warning(
            f"HTTP error fetching matches for {tournament_pagename}: "
            f"{e.response.status_code if e.response else 'unknown'}"
        )
        # Re-raise to let retry handler work
        raise

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response for {tournament_pagename}: {e}")
        raise APIException(
            f"Invalid JSON in API response for tournament {tournament_pagename}",
            url=BASE_URLS["match"],
            params=params,
            response_body=r.text[:500] if r else None
        ) from e

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {tournament_pagename}: {e}")
        raise NetworkException(
            f"Request failed for tournament {tournament_pagename}",
            url=BASE_URLS["match"],
            params=params
        ) from e


def fetch_all_matches_for_tournament(tournament_pagename):
    """
    Paginate through all matches for a single tournament.

    Args:
        tournament_pagename: Tournament identifier

    Returns:
        List of all matches for the tournament (empty list if tournament fails)
    """
    all_matches = []
    offset = 0
    limit = 1000

    logger.info(f"Starting match fetch for tournament: {tournament_pagename}")

    try:
        while True:
            data = fetch_matches(tournament_pagename, offset=offset, limit=limit)
            matches = data.get("result", [])

            if not matches:
                break

            all_matches.extend(matches)

            if len(matches) < limit:
                break

            offset += limit

        logger.info(f"Fetched {len(all_matches)} matches for {tournament_pagename}")
        return all_matches

    except (APIException, NetworkException) as e:
        # Log error but return empty list (don't let one tournament stop the pipeline)
        logger.warning(
            f"Failed to fetch matches for tournament {tournament_pagename} after retries: {e}"
        )
        return []

    except Exception as e:
        # Catch any unexpected errors
        logger.error(
            f"Unexpected error fetching matches for {tournament_pagename}: {e}"
        )
        return []