import requests
from src.config import BASE_URLS, HEADERS, CONNECT_TIMEOUT, READ_TIMEOUT
from src.utils.retry_handler import retry_with_backoff
from src.utils.logger import get_fetch_logger
from src.utils.exceptions import APIException, NetworkException
import json

logger = get_fetch_logger()


@retry_with_backoff(max_retries=3, base_delay=1.0)
def fetch_tournaments(start_date, end_date, limit=1000, offset=0):
    """
    Fetch tournaments from Liquipedia API.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Maximum number of results per page
        offset: Pagination offset

    Returns:
        API response as dictionary

    Raises:
        APIException: For API-specific errors
        NetworkException: For network-related errors
    """
    conditions = (
        f"[[startdate::>{start_date}]] AND [[startdate::<{end_date}]]"
    )

    params = {
        "wiki": "counterstrike",
        "limit": limit,
        "conditions": conditions,
        "order": "startdate ASC",
        "offset": offset,
    }

    try:
        logger.debug(f"Fetching tournaments: offset={offset}, limit={limit}")
        r = requests.get(
            BASE_URLS["tournament"],
            headers=HEADERS,
            params=params,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        r.raise_for_status()
        return r.json()

    except requests.exceptions.Timeout as e:
        logger.error(f"Request timeout while fetching tournaments: {e}")
        raise NetworkException(
            "Request timed out",
            url=BASE_URLS["tournament"],
            params=params
        ) from e

    except requests.exceptions.HTTPError as e:
        # Let retry handler deal with retryable errors
        raise

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response from API: {e}")
        raise APIException(
            "Invalid JSON in API response",
            url=BASE_URLS["tournament"],
            params=params,
            response_body=r.text[:500] if r else None
        ) from e

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise NetworkException(
            "Request failed",
            url=BASE_URLS["tournament"],
            params=params
        ) from e


def fetch_all_cs2_tournaments(
    start_date="2023-10-15",
    end_date="2025-12-01",
    limit=1000
):
    """
    Fetch all CS2 tournaments using offset-based pagination.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Results per page

    Returns:
        List of all tournaments

    Raises:
        PaginationException: If pagination appears to be stuck in infinite loop
    """
    from src.config import MAX_PAGINATION_ITERATIONS
    from src.utils.exceptions import PaginationException

    all_tournaments = []
    seen_tournament_ids = set()
    offset = 0
    iteration = 0

    logger.info(f"Starting tournament fetch: {start_date} to {end_date}")

    while iteration < MAX_PAGINATION_ITERATIONS:
        iteration += 1

        try:
            data = fetch_tournaments(start_date, end_date, limit=limit, offset=offset)
            tournaments = data.get("result", [])

            if not tournaments:
                logger.info(f"No more tournaments found at offset {offset}")
                break

            # Track unique tournaments to detect duplicates
            new_tournaments = 0
            for tournament in tournaments:
                tournament_id = tournament.get("pagename") or tournament.get("tournament")
                if tournament_id and tournament_id not in seen_tournament_ids:
                    seen_tournament_ids.add(tournament_id)
                    all_tournaments.append(tournament)
                    new_tournaments += 1

            logger.info(
                f"Iteration {iteration}: Fetched {len(tournaments)} tournaments "
                f"({new_tournaments} new), offset={offset}, total={len(all_tournaments)}"
            )

            # Stop if we got fewer results than the limit (last page)
            if len(tournaments) < limit:
                logger.info(f"Reached last page (got {len(tournaments)} < {limit})")
                break

            # Stop if no new tournaments were found (all duplicates)
            if new_tournaments == 0:
                logger.warning(f"No new tournaments at offset {offset}, stopping pagination")
                break

            # Increment offset for next page
            offset += limit

        except (APIException, NetworkException) as e:
            logger.error(f"Failed to fetch tournaments at offset {offset}: {e}")
            raise

    if iteration >= MAX_PAGINATION_ITERATIONS:
        logger.error(f"Hit maximum pagination iterations ({MAX_PAGINATION_ITERATIONS})")
        raise PaginationException(
            f"Exceeded maximum pagination iterations: {MAX_PAGINATION_ITERATIONS}",
            params={"start_date": start_date, "end_date": end_date, "offset": offset}
        )

    logger.info(f"Tournament fetch complete: {len(all_tournaments)} unique tournaments")
    return all_tournaments
