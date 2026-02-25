"""
One-time migration: import existing CSV data into the SQLite database.

Safe to run multiple times (idempotent via INSERT OR REPLACE).

Usage:
    python -m scripts.migrate_csv_to_sqlite
"""

import sys
import pandas as pd
from src.db.database import initialize_db, upsert_tournaments, upsert_matches, get_match_count

TOURNAMENTS_CSV = "data/processed/cs2_tournaments.csv"
MATCHES_CSV = "data/processed/cs2_matches_deduplicated.csv"

# Columns kept in DB (tournament_id and location are dropped)
TOURNAMENT_DB_COLS = ["pagename", "name", "startdate", "enddate", "tier", "prizepool", "type", "game"]
MATCH_DB_COLS = [
    "match_id", "tournament_pagename", "date", "bestof",
    "team1_id", "team1_name", "team1_score",
    "team2_id", "team2_name", "team2_score",
    "winner_id", "team1_win", "team2_win",
]


def _to_str_col(series: pd.Series) -> pd.Series:
    """Convert a datetime/Timestamp column to ISO string, leaving None/NaT as None."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.where(series.notna(), other=None).astype(object).apply(
            lambda v: v.strftime("%Y-%m-%d") if v is not None else None
        )
    return series


def migrate_tournaments():
    try:
        df = pd.read_csv(TOURNAMENTS_CSV)
    except FileNotFoundError:
        print(f"[WARN] Tournaments CSV not found at {TOURNAMENTS_CSV}, skipping.")
        return 0

    original = len(df)
    # Drop columns that don't belong in DB
    for col in ["tournament_id", "location"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Ensure all expected columns exist (fill missing with None)
    for col in TOURNAMENT_DB_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[TOURNAMENT_DB_COLS]

    # Normalize date columns to plain strings
    for col in ["startdate", "enddate"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df[col] = _to_str_col(df[col])

    # Drop rows with no pagename (primary key)
    df = df[df["pagename"].notna()]
    dropped = original - len(df)
    if dropped:
        print(f"[INFO] Dropped {dropped} tournament rows with null pagename")

    records = df.to_dict("records")
    upsert_tournaments(records)
    print(f"[OK] Upserted {len(records)} tournaments into DB")
    return len(records)


def migrate_matches():
    try:
        df = pd.read_csv(MATCHES_CSV)
    except FileNotFoundError:
        print(f"[WARN] Matches CSV not found at {MATCHES_CSV}, skipping.")
        return 0

    original = len(df)

    # Ensure all expected columns exist
    for col in MATCH_DB_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[MATCH_DB_COLS]

    # Normalize date to plain string
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["date"] = df["date"].where(df["date"].notna(), other=None).astype(object).apply(
        lambda v: v.strftime("%Y-%m-%d %H:%M:%S") if v is not None else None
    )

    # Drop rows with no match_id (primary key)
    df = df[df["match_id"].notna()]
    dropped = original - len(df)
    if dropped:
        print(f"[INFO] Dropped {dropped} match rows with null match_id")

    records = df.to_dict("records")
    upsert_matches(records)
    print(f"[OK] Upserted {len(records)} matches into DB")
    return len(records)


def main():
    print("=" * 60)
    print("CS2 Predictor — CSV → SQLite Migration")
    print("=" * 60)

    initialize_db()
    print(f"[OK] Database initialized at data/processed/cs2_data.db")

    t_count = migrate_tournaments()
    m_count = migrate_matches()

    total_in_db = get_match_count()
    print("=" * 60)
    print(f"Migration complete.")
    print(f"  Tournaments upserted : {t_count}")
    print(f"  Matches upserted     : {m_count}")
    print(f"  Total matches in DB  : {total_in_db}")
    print("=" * 60)


if __name__ == "__main__":
    main()
