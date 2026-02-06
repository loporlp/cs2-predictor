"""
Deduplication utility for match data.

This script creates a deduplicated version of the raw match data.
The raw data in cs2_matches.csv is preserved - this creates a new file.

Usage:
    python -m src.pipelines.deduplicate_matches
    python -m src.pipelines.deduplicate_matches --output data/processed/cs2_matches_clean.csv
    python -m src.pipelines.deduplicate_matches --strategy latest  # Keep most recent duplicate
    python -m src.pipelines.deduplicate_matches --strategy first   # Keep first occurrence (default)
"""

import os
import sys
import pandas as pd
from src.utils.logger import get_pipeline_logger

logger = get_pipeline_logger()

# Default paths
INPUT_PATH = "data/processed/cs2_matches.csv"
OUTPUT_PATH = "data/processed/cs2_matches_deduplicated.csv"


def deduplicate_matches(input_path=INPUT_PATH, output_path=OUTPUT_PATH, strategy="first"):
    """
    Create a deduplicated version of match data.

    Args:
        input_path: Path to raw match data CSV
        output_path: Path to save deduplicated data
        strategy: Deduplication strategy
                 - "first": Keep first occurrence of each match_id (default)
                 - "latest": Keep most recent occurrence (by date)

    Returns:
        None
    """
    logger.info("=" * 80)
    logger.info("Match Deduplication Utility")
    logger.info("=" * 80)
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    logger.info(f"Strategy: {strategy}")

    # Load raw match data
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        raise FileNotFoundError(f"No match data found at {input_path}")

    try:
        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} raw matches")
    except Exception as e:
        logger.error(f"Failed to load match data: {e}")
        raise

    if df.empty:
        logger.warning("Input file is empty, nothing to deduplicate")
        return

    # Convert date to datetime for sorting (if using latest strategy)
    if strategy == "latest":
        df["date"] = pd.to_datetime(df["date"], errors='coerce')
        # Sort by date descending so most recent comes first
        df = df.sort_values("date", ascending=False, na_position='last')

    # Count duplicates before deduplication
    original_count = len(df)
    unique_match_ids = df["match_id"].nunique()
    duplicate_count = original_count - unique_match_ids

    logger.info(f"Unique match IDs: {unique_match_ids}")
    logger.info(f"Duplicate entries: {duplicate_count}")

    if duplicate_count == 0:
        logger.info("No duplicates found, saving copy of original data")
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} matches to {output_path}")
        return

    # Report duplicates by match_id
    duplicate_ids = df[df.duplicated(subset=["match_id"], keep=False)]["match_id"].unique()
    logger.info(f"Found {len(duplicate_ids)} match IDs with duplicates")

    # Show some examples
    if len(duplicate_ids) > 0:
        logger.info("Example duplicate match IDs:")
        for mid in list(duplicate_ids)[:5]:
            count = len(df[df["match_id"] == mid])
            logger.info(f"  - {mid}: {count} occurrences")

    # Deduplicate (keep='first' if strategy is 'first', otherwise already sorted by date)
    df_clean = df.drop_duplicates(subset=["match_id"], keep='first').reset_index(drop=True)

    # Save deduplicated data
    df_clean.to_csv(output_path, index=False)

    # Summary
    logger.info("=" * 80)
    logger.info("Deduplication Summary")
    logger.info("=" * 80)
    logger.info(f"Original raw matches: {original_count}")
    logger.info(f"Duplicates removed: {duplicate_count}")
    logger.info(f"Final clean matches: {len(df_clean)}")
    logger.info(f"Output saved to: {output_path}")
    logger.info("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Deduplicate CS2 match data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.pipelines.deduplicate_matches
  python -m src.pipelines.deduplicate_matches --output custom_output.csv
  python -m src.pipelines.deduplicate_matches --strategy latest
        """
    )

    parser.add_argument(
        "--input",
        default=INPUT_PATH,
        help=f"Path to raw match data CSV (default: {INPUT_PATH})"
    )

    parser.add_argument(
        "--output",
        default=OUTPUT_PATH,
        help=f"Path to save deduplicated data (default: {OUTPUT_PATH})"
    )

    parser.add_argument(
        "--strategy",
        choices=["first", "latest"],
        default="first",
        help="Deduplication strategy: 'first' keeps first occurrence, 'latest' keeps most recent by date"
    )

    args = parser.parse_args()

    try:
        deduplicate_matches(
            input_path=args.input,
            output_path=args.output,
            strategy=args.strategy
        )
    except Exception as e:
        logger.error(f"Deduplication failed: {e}")
        sys.exit(1)
