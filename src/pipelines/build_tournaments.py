from src.db.database import initialize_db, upsert_tournaments
from src.fetch.tournaments import fetch_all_cs2_tournaments
from src.parse.tournaments import normalize_tournament
from src.utils.logger import get_pipeline_logger
from src.utils.validators import is_valid_date

logger = get_pipeline_logger()


def build_tournament_table(start_date="2023-10-15", end_date="2025-12-01"):
    """
    Build tournament table by fetching, parsing, and saving tournament data.

    Args:
        start_date: Start date for tournament fetch (YYYY-MM-DD)
        end_date: End date for tournament fetch (YYYY-MM-DD)

    Returns:
        None
    """
    initialize_db()

    logger.info("=" * 80)
    logger.info("Starting tournament pipeline")
    logger.info("=" * 80)
    logger.info(f"Date range: {start_date} to {end_date}")

    # Fetch raw tournament data
    try:
        raw = fetch_all_cs2_tournaments(start_date=start_date, end_date=end_date)
        logger.info(f"Fetched {len(raw)} raw tournaments from API")
    except Exception as e:
        logger.error(f"Failed to fetch tournaments: {e}")
        raise

    # Parse and validate tournament data
    parsed = []
    invalid_count = 0

    for t in raw:
        result = normalize_tournament(t)
        if result is not None:
            parsed.append(result)
        else:
            invalid_count += 1

    logger.info(
        f"Parsed {len(parsed)} valid tournaments, {invalid_count} invalid tournaments filtered out"
    )

    if not parsed:
        logger.error("No valid tournaments found, cannot insert into DB")
        raise ValueError("No valid tournament data to process")

    # Validate dates before upsert
    invalid_start_dates = 0
    invalid_end_dates = 0

    for r in parsed:
        if r.get("startdate") and not is_valid_date(str(r["startdate"])):
            invalid_start_dates += 1
            logger.warning(f"Invalid start date for tournament {r.get('pagename')}: {r['startdate']}")

        if r.get("enddate") and not is_valid_date(str(r["enddate"])):
            invalid_end_dates += 1
            logger.warning(f"Invalid end date for tournament {r.get('pagename')}: {r['enddate']}")

    if invalid_start_dates > 0:
        logger.warning(f"Found {invalid_start_dates} tournaments with invalid start dates")
    if invalid_end_dates > 0:
        logger.warning(f"Found {invalid_end_dates} tournaments with invalid end dates")

    # Upsert into DB
    upsert_tournaments(parsed)
    logger.info(f"Upserted {len(parsed)} tournaments into DB")

    # Summary statistics
    logger.info("=" * 80)
    logger.info("TOURNAMENT PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total raw tournaments fetched: {len(raw)}")
    logger.info(f"Valid tournaments parsed: {len(parsed)}")
    logger.info(f"Invalid tournaments filtered: {invalid_count}")
    logger.info(f"Invalid start dates: {invalid_start_dates}")
    logger.info(f"Invalid end dates: {invalid_end_dates}")
    logger.info(f"Final tournament count: {len(parsed)}")
    logger.info("Output: data/processed/cs2_data.db (tournaments table)")
    logger.info("=" * 80)


if __name__ == "__main__":
    import sys
    from datetime import datetime

    # Parse command-line arguments
    start_date = "2023-10-15"  # CS2 release date
    end_date = datetime.now().strftime("%Y-%m-%d")

    if "--start-date" in sys.argv:
        try:
            idx = sys.argv.index("--start-date")
            start_date = sys.argv[idx + 1]
            logger.info(f"Using custom start date: {start_date}")
        except (IndexError, ValueError):
            logger.error("--start-date requires a date argument (YYYY-MM-DD)")
            sys.exit(1)

    if "--end-date" in sys.argv:
        try:
            idx = sys.argv.index("--end-date")
            end_date = sys.argv[idx + 1]
            logger.info(f"Using custom end date: {end_date}")
        except (IndexError, ValueError):
            logger.error("--end-date requires a date argument (YYYY-MM-DD)")
            sys.exit(1)

    build_tournament_table(start_date=start_date, end_date=end_date)
