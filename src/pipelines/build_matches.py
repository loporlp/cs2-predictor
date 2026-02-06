import os
import pandas as pd
from datetime import datetime, timedelta
from src.fetch.matches import fetch_all_matches_for_tournament
from src.parse.matches import normalize_match
from src.utils.logger import get_pipeline_logger
from src.utils.validators import is_valid_date

logger = get_pipeline_logger()

# Paths for input (tournaments) and output (matches)
INPUT_PATH = "data/processed/cs2_tournaments.csv"
OUTPUT_PATH = "data/processed/cs2_matches.csv"

# Buffer for incremental fetches (fetch from most_recent_date - INCREMENTAL_BUFFER_DAYS)
INCREMENTAL_BUFFER_DAYS = 30


def load_existing_matches():
    """
    Load existing matches from the output file if it exists.

    Returns:
        tuple: (DataFrame of existing matches, set of processed tournament pagenames)
    """
    if os.path.exists(OUTPUT_PATH):
        try:
            df_existing = pd.read_csv(OUTPUT_PATH)
            processed_tournaments = set(df_existing["tournament_pagename"].dropna().unique())
            logger.info(f"Loaded {len(df_existing)} existing matches from {len(processed_tournaments)} tournaments")
            return df_existing, processed_tournaments
        except Exception as e:
            logger.warning(f"Could not load existing matches: {e}. Starting fresh.")
            return pd.DataFrame(), set()
    return pd.DataFrame(), set()


def get_most_recent_match_date(df_existing):
    """
    Get the most recent match date from existing matches.

    Args:
        df_existing: DataFrame of existing matches

    Returns:
        datetime or None: Most recent match date, or None if no valid dates found
    """
    if df_existing.empty or "date" not in df_existing.columns:
        return None

    try:
        # Convert to datetime if not already
        dates = pd.to_datetime(df_existing["date"], errors='coerce')
        valid_dates = dates.dropna()

        if valid_dates.empty:
            return None

        most_recent = valid_dates.max()
        logger.info(f"Most recent match date in existing data: {most_recent}")
        return most_recent

    except Exception as e:
        logger.warning(f"Could not determine most recent match date: {e}")
        return None


def save_matches_incrementally(df_matches):
    """
    Save matches to the output file, overwriting any existing data.
    NO DEDUPLICATION - all raw data is preserved.

    Args:
        df_matches: DataFrame of matches to save
    """
    df_matches.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"Checkpoint saved: {len(df_matches)} total matches (raw, no deduplication) to {OUTPUT_PATH}")


def build_match_table(incremental=False, tournament_start_date=None):
    """
    Build match table by fetching, parsing, and saving match data.

    Supports incremental processing - if the pipeline is interrupted,
    it can be restarted and will skip tournaments that have already
    been processed.

    Args:
        incremental: If True, only process tournaments from recent date range
        tournament_start_date: Override start date for tournaments (YYYY-MM-DD)
                              If None and incremental=True, uses most recent match date - buffer

    Returns:
        None

    Note:
        NO DEDUPLICATION is performed. All raw match data is preserved.
        Use the separate deduplicate_matches.py script for clean data.
    """
    logger.info("=" * 80)
    logger.info("Starting match pipeline")
    if incremental:
        logger.info("MODE: INCREMENTAL (recent tournaments only)")
    else:
        logger.info("MODE: FULL HISTORY")
    logger.info("=" * 80)

    # Load the tournament data built in the previous step
    try:
        df_tournaments = pd.read_csv(INPUT_PATH)
        logger.info(f"Loaded {len(df_tournaments)} tournaments from {INPUT_PATH}")
    except FileNotFoundError:
        logger.error(f"Tournament file not found at {INPUT_PATH}. Run build_tournaments.py first.")
        raise

    # Load existing matches to resume from where we left off
    df_existing, processed_tournaments = load_existing_matches()
    all_matches_list = []
    if not df_existing.empty:
        all_matches_list = df_existing.to_dict('records')

    # Handle incremental mode
    if incremental:
        if tournament_start_date:
            cutoff_date = tournament_start_date
            logger.info(f"Using manual cutoff date: {cutoff_date}")
        else:
            most_recent_match = get_most_recent_match_date(df_existing)
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

            # In incremental mode, don't skip already processed tournaments
            # (we want to re-fetch them to catch updates)
            processed_tournaments = set()

    # Filter out any tournaments without a pagename, as we can't query matches for them
    all_tournaments_with_pagename = df_tournaments[df_tournaments["pagename"].notna()]["pagename"].tolist()

    # Filter out already processed tournaments
    tournaments_to_process = [t for t in all_tournaments_with_pagename if t not in processed_tournaments]

    tournaments_without_pagename = len(df_tournaments) - len(all_tournaments_with_pagename)
    if tournaments_without_pagename > 0:
        logger.warning(
            f"Skipping {tournaments_without_pagename} tournaments without pagenames"
        )

    already_processed_count = len(all_tournaments_with_pagename) - len(tournaments_to_process)
    if already_processed_count > 0:
        logger.info(f"Skipping {already_processed_count} tournaments already processed (resuming from checkpoint)")

    logger.info(f"Processing {len(tournaments_to_process)} tournaments with pagenames")

    if not tournaments_to_process:
        logger.info("All tournaments already processed. Nothing to do.")
        if all_matches_list:
            logger.info(f"Existing data has {len(all_matches_list)} matches from {len(processed_tournaments)} tournaments")
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
            # Parse matches for this tournament immediately
            for m in raw_matches:
                result = normalize_match(m)
                if result is not None:
                    all_matches_list.append(result)
                    new_matches_count += 1
                else:
                    invalid_matches += 1
            successful_tournaments += 1
        else:
            # Empty list could mean no matches or a failure
            # The fetch function logs warnings for failures
            failed_tournaments += 1

        # Save checkpoint after each tournament
        if raw_matches:
            df_checkpoint = pd.DataFrame(all_matches_list)
            save_matches_incrementally(df_checkpoint)

    logger.info(f"Fetched {new_matches_count} new matches from {len(tournaments_to_process) - failed_tournaments} tournaments")
    if failed_tournaments > 0:
        logger.warning(f"{failed_tournaments} tournaments returned no matches (may have failed or have no matches)")

    if not all_matches_list:
        logger.error("No matches fetched, cannot create CSV")
        raise ValueError("No match data to process")

    logger.info("Finalizing data...")

    # Create the final DataFrame
    df_matches = pd.DataFrame(all_matches_list)

    # Validate dates before coercion
    invalid_dates = 0
    for _, row in df_matches.iterrows():
        if row["date"] and not pd.isna(row["date"]):
            date_str = str(row["date"])
            # Try both formats
            if not is_valid_date(date_str) and not is_valid_date(date_str, "%Y-%m-%d %H:%M:%S"):
                invalid_dates += 1
                logger.warning(
                    f"Invalid date for match {row.get('match_id')}: {row['date']}"
                )

    if invalid_dates > 0:
        logger.warning(f"Found {invalid_dates} matches with invalid dates (will be coerced to NaT)")

    # Convert date to datetime and handle invalid entries
    df_matches["date"] = pd.to_datetime(df_matches["date"], errors='coerce')

    # Count potential duplicates for reporting (but DO NOT remove them)
    original_count = len(df_matches)
    unique_match_ids = df_matches["match_id"].nunique()
    potential_duplicates = original_count - unique_match_ids

    if potential_duplicates > 0:
        logger.info(f"Dataset contains {potential_duplicates} potential duplicate matches (will NOT be removed)")
        logger.info(f"Total matches: {original_count}, Unique match IDs: {unique_match_ids}")
        logger.info("Use deduplicate_matches.py script to create a clean dataset when needed")

    # Save the final table WITHOUT deduplication
    df_matches.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"Saved {len(df_matches)} total matches (raw data, no deduplication) to {OUTPUT_PATH}")

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
    logger.info(f"Invalid dates coerced: {invalid_dates}")
    logger.info(f"Potential duplicates detected: {potential_duplicates}")
    logger.info(f"Total raw match count: {len(df_matches)}")
    logger.info(f"Unique match IDs: {unique_match_ids}")
    logger.info(f"Output file: {OUTPUT_PATH}")
    logger.info("NOTE: No deduplication performed. Use deduplicate_matches.py for clean data.")
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