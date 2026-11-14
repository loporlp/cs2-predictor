"""
Central SQLite database module for CS2 predictor.

All pipeline files import from here — no pipeline imports sqlite3 directly.
DB path: data/processed/cs2_data.db
"""

import sqlite3
from contextlib import contextmanager

import pandas as pd

DB_PATH = "data/processed/cs2_data.db"

_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tournaments (
    pagename  TEXT PRIMARY KEY NOT NULL,
    name      TEXT,
    startdate TEXT,
    enddate   TEXT,
    tier      INTEGER,
    prizepool REAL,
    type      TEXT,
    game      TEXT
);
CREATE INDEX IF NOT EXISTS idx_tournaments_startdate ON tournaments (startdate);
CREATE INDEX IF NOT EXISTS idx_tournaments_tier      ON tournaments (tier);

CREATE TABLE IF NOT EXISTS matches (
    match_id            TEXT PRIMARY KEY NOT NULL,
    tournament_pagename TEXT REFERENCES tournaments(pagename),
    date                TEXT,
    bestof              INTEGER,
    team1_id            INTEGER,
    team1_name          TEXT,
    team1_score         INTEGER,
    team2_id            INTEGER,
    team2_name          TEXT,
    team2_score         INTEGER,
    winner_id           INTEGER,
    team1_win           INTEGER,
    team2_win           INTEGER
);
CREATE INDEX IF NOT EXISTS idx_matches_date       ON matches (date);
CREATE INDEX IF NOT EXISTS idx_matches_tournament ON matches (tournament_pagename);
CREATE INDEX IF NOT EXISTS idx_matches_team1      ON matches (team1_name);
CREATE INDEX IF NOT EXISTS idx_matches_team2      ON matches (team2_name);

CREATE TABLE IF NOT EXISTS fetch_log (
    tournament_pagename TEXT PRIMARY KEY NOT NULL,
    fetched_at          TEXT
);
"""

# Migration: treat all tournaments that already have matches as fully fetched.
# Runs once on first upgrade; INSERT OR IGNORE is a no-op on subsequent runs.
_MIGRATION_SQL = """
INSERT OR IGNORE INTO fetch_log (tournament_pagename, fetched_at)
SELECT DISTINCT tournament_pagename, 'migrated'
FROM matches
WHERE tournament_pagename IS NOT NULL;
"""

_TOURNAMENT_COLUMNS = ["pagename", "name", "startdate", "enddate", "tier", "prizepool", "type", "game"]
_MATCH_COLUMNS = [
    "match_id", "tournament_pagename", "date", "bestof",
    "team1_id", "team1_name", "team1_score",
    "team2_id", "team2_name", "team2_score",
    "winner_id", "team1_win", "team2_win",
]


def initialize_db():
    """Create tables and indexes if they don't exist (idempotent)."""
    import os
    os.makedirs("data/processed", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_MIGRATION_SQL)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_db():
    """Context manager that yields a connection, commits on exit, rolls back on exception."""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_connection():
    """Return a raw connection for use with pd.read_sql_query(). Caller must close it."""
    return sqlite3.connect(DB_PATH)


def _to_str(value):
    """Convert a value to str for storage, keeping None as None."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN check
        return None
    s = str(value)
    # pandas Timestamp -> ISO string; strip extra precision
    if s in ("NaT", "nan", "None", ""):
        return None
    return s


def upsert_tournaments(records: list[dict]):
    """Batch INSERT OR REPLACE into tournaments table."""
    if not records:
        return
    rows = []
    for r in records:
        row = tuple(r.get(col) for col in _TOURNAMENT_COLUMNS)
        rows.append(row)
    placeholders = ", ".join(["?"] * len(_TOURNAMENT_COLUMNS))
    sql = f"INSERT OR REPLACE INTO tournaments ({', '.join(_TOURNAMENT_COLUMNS)}) VALUES ({placeholders})"
    with get_db() as conn:
        conn.executemany(sql, rows)


def upsert_matches(records: list[dict]):
    """Batch INSERT OR REPLACE into matches table (replaces deduplication step)."""
    if not records:
        return
    rows = []
    for r in records:
        row = tuple(r.get(col) for col in _MATCH_COLUMNS)
        rows.append(row)
    placeholders = ", ".join(["?"] * len(_MATCH_COLUMNS))
    sql = f"INSERT OR REPLACE INTO matches ({', '.join(_MATCH_COLUMNS)}) VALUES ({placeholders})"
    with get_db() as conn:
        conn.executemany(sql, rows)


def get_processed_tournament_pagenames() -> set:
    """Return the set of tournament pagenames that already have matches in the DB."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT DISTINCT tournament_pagename FROM matches WHERE tournament_pagename IS NOT NULL",
            conn,
        )
        return set(df["tournament_pagename"].tolist())
    finally:
        conn.close()


def get_fetch_completed_pagenames() -> set:
    """Return pagenames that have a completed fetch record in fetch_log."""
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT tournament_pagename FROM fetch_log", conn)
        return set(df["tournament_pagename"].tolist())
    finally:
        conn.close()


def mark_tournament_fetched(pagename: str):
    """Record that a tournament's fetch completed successfully."""
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO fetch_log (tournament_pagename, fetched_at) VALUES (?, datetime('now'))",
            (pagename,),
        )


def get_most_recent_match_date():
    """Return the most recent match date as a pd.Timestamp, or None."""
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT MAX(date) AS max_date FROM matches", conn)
        val = df["max_date"].iloc[0]
        if val is None or (isinstance(val, float) and val != val):
            return None
        ts = pd.Timestamp(val)
        return ts if not pd.isna(ts) else None
    finally:
        conn.close()


def load_tournaments_df() -> pd.DataFrame:
    """Load all tournaments as a DataFrame ordered by startdate ASC."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM tournaments ORDER BY startdate ASC",
            conn,
        )
    finally:
        conn.close()


def get_recent_matches(limit: int = 50, offset: int = 0, team: str = "") -> tuple[list[dict], int]:
    """Return recent completed matches (winner_id 1 or 2), newest first, with optional team filter."""
    conn = get_connection()
    try:
        if team:
            pattern = f"%{team}%"
            df = pd.read_sql_query(
                """SELECT date, team1_name, team1_score, team2_name, team2_score,
                          winner_id, bestof, tournament_pagename
                   FROM matches
                   WHERE (team1_name LIKE ? OR team2_name LIKE ?) AND winner_id IN (1, 2)
                   ORDER BY date DESC LIMIT ? OFFSET ?""",
                conn, params=(pattern, pattern, limit, offset),
            )
            total = int(pd.read_sql_query(
                "SELECT COUNT(*) AS cnt FROM matches WHERE (team1_name LIKE ? OR team2_name LIKE ?) AND winner_id IN (1, 2)",
                conn, params=(pattern, pattern),
            )["cnt"].iloc[0])
        else:
            df = pd.read_sql_query(
                """SELECT date, team1_name, team1_score, team2_name, team2_score,
                          winner_id, bestof, tournament_pagename
                   FROM matches WHERE winner_id IN (1, 2)
                   ORDER BY date DESC LIMIT ? OFFSET ?""",
                conn, params=(limit, offset),
            )
            total = int(pd.read_sql_query(
                "SELECT COUNT(*) AS cnt FROM matches WHERE winner_id IN (1, 2)", conn
            )["cnt"].iloc[0])
        return df.to_dict(orient="records"), total
    finally:
        conn.close()


def load_matches_df() -> pd.DataFrame:
    """Load all matches as a DataFrame ordered by date ASC."""
    conn = get_connection()
    try:
        return pd.read_sql_query(
            "SELECT * FROM matches ORDER BY date ASC",
            conn,
        )
    finally:
        conn.close()


def get_match_count() -> int:
    """Return total number of rows in matches table."""
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT COUNT(*) AS cnt FROM matches", conn)
        return int(df["cnt"].iloc[0])
    finally:
        conn.close()
