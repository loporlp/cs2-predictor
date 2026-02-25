import pandas as pd
from datetime import timedelta
from src.db.database import (
    initialize_db,
    load_tournaments_df,
    get_processed_tournament_pagenames,
    get_most_recent_match_date,
    upsert_matches,
    get_match_count,
)
from src.fetch.matches import fetch_all_matches_for_tournament
from src.parse.matches import normalize_match
from src.utils.logger import get_pipeline_logger
from src.utils.validators import is_valid_date

logger = get_pipeline_logger()

# Buffer for incremental fetches (fetch from most_recent_date - INCREMENTAL_BUFFER_DAYS)
INCREMENTAL_BUFFER_DAYS = 30


def build_match_table(incremental=False, tournament_start_date=None):
    """
    Build match table by fetching, parsing, and upserting match data into SQLite.

    Supports incremental processing — if the pipeline is interrupted,
    it can be restarted and will skip tournaments that have already
    been processed.

    Args:
        incremental: If True, only process tournaments from recent date range
        tournament_start_date: Override start date for tournaments (YYYY-MM-DD).
                               If None and incremental=True, uses most recent match date - buffer.

    Returns:
        None
    """
    initialize_db()

    logger.info("=" * 80)
    logger.info("Starting match pipeline")
    if incremental:
        logger.info("MODE: INCREMENTAL (recent tournaments only)")
    else:
        logger.info("MODE: FULL HISTORY")
    logger.info("=" * 80)

    # Load the tournament data from DB
    df_tournaments = load_tournaments_df()
    logger.info(f"Loaded {len(df_tournaments)} tournaments from DB")

    # Load processed pagenames to resume from checkpoint
    processed_tournaments = get_processed_tournament_pagenames()
    if processed_tournaments:
        logger.info(f"Found {len(processed_tournaments)} already-processed tournaments in DB")

    # Handle incremental mode
    if incremental:
        if tournament_start_date:
            cutoff_date = tournament_start_date
            logger.info(f"Using manual cutoff date: {cutoff_date}")
        else:
            most_recent_match = get_most_recent_match_date()
            if most_recent_match:
                cutoff_datetime = most_recent_match - timedelta(days=INCREMENTAL_BUFFER_DAYS)
                cutoff_date = cutoff_datetime.strftime("%Y-%m-%d")
                logger.info(f"Calculated cutoff date: {cutoff_date} (most recent - {INCREMENTAL_BUFFER_DAYS} days)")
            else:
                logger.warning("No existing match dates found, falling back to full history mode")
                incremental = False
                cutoff_date = None

        if incremental and cutoff_date:
            # Filter tournaments to only those starting after cutoff
            df_tournaments["startdate"] = pd.to_datetime(df_tournaments["startdate"], errors='coerce')
            original_count = len(df_tournaments)
            df_tournaments = df_tournaments[df_tournaments["startdate"] >= cutoff_date]
            logger.info(f"Filtered to {len(df_tournaments)} tournaments after {cutoff_date} (was {original_count})")

            # In incremental mode, re-fetch all filtered tournaments (to catch updates)
            processed_tournaments = set()

    # Filter out any tournaments without a pagename
    all_tournaments_with_pagename = df_tournaments[df_tournaments["pagename"].notna()]["pagename"].tolist()

    # Filter out already processed tournaments
    tournaments_to_process = [t for t in all_tournaments_with_pagename if t not in processed_tournaments]

    tournaments_without_pagename = len(df_tournaments) - len(all_tournaments_with_pagename)
    if tournaments_without_pagename > 0:
        logger.warning(f"Skipping {tournaments_without_pagename} tournaments without pagenames")

    already_processed_count = len(all_tournaments_with_pagename) - len(tournaments_to_process)
    if already_processed_count > 0:
        logger.info(f"Skipping {already_processed_count} tournaments already processed (resuming from checkpoint)")

    logger.info(f"Processing {len(tournaments_to_process)} tournaments with pagenames")

    if not tournaments_to_process:
        logger.info("All tournaments already processed. Nothing to do.")
        logger.info(f"Total matches in DB: {get_match_count()}")
        return

    # Fetch matches for remaining tournaments
    failed_tournaments = 0
    successful_tournaments = already_processed_count
    invalid_matches = 0
    new_matches_count = 0

    for i, pagename in enumerate(tournaments_to_process, start=1):
        logger.info(f"[{i}/{len(tournaments_to_process)}] Processing tournament: {pagename}")
        raw_matches = fetch_all_matches_for_tournament(pagename)

        if raw_matches:
            parsed = []
            for m in raw_matches:
                result = normalize_match(m)
                if result is not None:
                    parsed.append(result)
                    new_matches_count += 1
                else:
                    invalid_matches += 1

            # Upsert this tournament's matches immediately (checkpoint per tournament)
            upsert_matches(parsed)
            successful_tournaments += 1
        else:
            failed_tournaments += 1

    logger.info(f"Fetched {new_matches_count} new matches from {len(tournaments_to_process) - failed_tournaments} tournaments")
    if failed_tournaments > 0:
        logger.warning(f"{failed_tournaments} tournaments returned no matches (may have failed or have no matches)")

    total_in_db = get_match_count()
    if total_in_db == 0:
        logger.error("No matches in DB after pipeline run")
        raise ValueError("No match data to process")

    # Summary statistics
    logger.info("=" * 80)
    logger.info("MATCH PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tournaments loaded: {len(df_tournaments)}")
    logger.info(f"Tournaments without pagenames: {tournaments_without_pagename}")
    logger.info(f"Tournaments already processed (from checkpoint): {already_processed_count}")
    logger.info(f"Tournaments processed this run: {len(tournaments_to_process)}")
    logger.info(f"Successful tournament fetches (total): {successful_tournaments}")
    logger.info(f"Failed/empty tournament fetches: {failed_tournaments}")
    logger.info(f"New matches fetched this run: {new_matches_count}")
    logger.info(f"Invalid matches filtered: {invalid_matches}")
    logger.info(f"Total matches in DB: {total_in_db}")
    logger.info("Output: data/processed/cs2_data.db (matches table)")
    logger.info("=" * 80)


if __name__ == "__main__":
    import sys

    # Parse command-line arguments
    incremental = "--incremental" in sys.argv
    tournament_start_date = None

    # Check for --from-date argument
    if "--from-date" in sys.argv:
        try:
            idx = sys.argv.index("--from-date")
            tournament_start_date = sys.argv[idx + 1]
            logger.info(f"Using custom start date: {tournament_start_date}")
        except (IndexError, ValueError):
            logger.error("--from-date requires a date argument (YYYY-MM-DD)")
            sys.exit(1)

    build_match_table(incremental=incremental, tournament_start_date=tournament_start_date)
