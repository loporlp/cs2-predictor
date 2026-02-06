import math
import os

import numpy as np
import pandas as pd

from src.features.elo import EloRatingSystem
from src.features.team_stats import TeamStatsTracker
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.features.build")

MATCHES_PATH = "data/processed/cs2_matches_deduplicated.csv"
TOURNAMENTS_PATH = "data/processed/cs2_tournaments.csv"
OUTPUT_PATH = "data/processed/feature_matrix.csv"
ELO_SAVE_PATH = "data/models/elo_ratings.json"
STATS_SAVE_PATH = "data/models/team_stats.json"

MIN_MATCHES = 5  # Minimum prior matches before a team enters training data


def load_and_clean_data():
    """Load match and tournament data, apply cleaning filters."""
    logger.info("Loading data...")
    df_matches = pd.read_csv(MATCHES_PATH)
    df_tournaments = pd.read_csv(TOURNAMENTS_PATH)
    logger.info(f"Loaded {len(df_matches)} matches, {len(df_tournaments)} tournaments")

    # Build tournament lookup: pagename -> {tier, prizepool, type}
    tournament_info = {}
    for _, row in df_tournaments.iterrows():
        pn = row.get("pagename")
        if pd.notna(pn):
            tournament_info[pn] = {
                "tier": row.get("tier"),
                "prizepool": row.get("prizepool"),
                "type": row.get("type"),
            }

    # Clean matches
    initial = len(df_matches)

    # Filter out winner_id == 0 (no winner determined)
    df_matches = df_matches[df_matches["winner_id"] != 0]
    logger.info(f"Filtered winner_id=0: {initial - len(df_matches)} removed")

    # Filter out scores of -1
    prev = len(df_matches)
    df_matches = df_matches[
        (df_matches["team1_score"] != -1) & (df_matches["team2_score"] != -1)
    ]
    logger.info(f"Filtered score=-1: {prev - len(df_matches)} removed")

    # Filter out missing team names
    prev = len(df_matches)
    df_matches = df_matches[
        df_matches["team1_name"].notna() & df_matches["team2_name"].notna()
        & (df_matches["team1_name"] != "") & (df_matches["team2_name"] != "")
    ]
    logger.info(f"Filtered missing names: {prev - len(df_matches)} removed")

    # Filter out missing dates
    prev = len(df_matches)
    df_matches["date"] = pd.to_datetime(df_matches["date"], errors="coerce")
    df_matches = df_matches[df_matches["date"].notna()]
    logger.info(f"Filtered missing dates: {prev - len(df_matches)} removed")

    # Sort chronologically
    df_matches = df_matches.sort_values("date").reset_index(drop=True)
    logger.info(f"Clean dataset: {len(df_matches)} matches, date range: {df_matches['date'].min()} to {df_matches['date'].max()}")

    return df_matches, tournament_info


def build_feature_matrix():
    """Build the feature matrix chronologically, preventing data leakage."""
    df_matches, tournament_info = load_and_clean_data()

    elo = EloRatingSystem()
    stats = TeamStatsTracker()

    features_list = []
    skipped_min_matches = 0

    logger.info("Building features chronologically...")

    for idx, row in df_matches.iterrows():
        team1 = row["team1_name"]
        team2 = row["team2_name"]
        team1_win = int(row["team1_win"])
        date_str = str(row["date"])
        tourney_pn = row.get("tournament_pagename")

        # Get tournament context
        t_info = tournament_info.get(tourney_pn, {})
        tier = t_info.get("tier")
        prizepool = t_info.get("prizepool")
        t_type = t_info.get("type")

        # Check minimum match threshold (extract features only if both teams qualify)
        t1_count = stats.get_total_matches(team1)
        t2_count = stats.get_total_matches(team2)

        if t1_count >= MIN_MATCHES and t2_count >= MIN_MATCHES:
            # Extract features BEFORE updating trackers (no leakage)
            feat = extract_features(elo, stats, team1, team2, date_str, tier, prizepool, t_type)
            feat["team1_win"] = team1_win
            feat["date"] = row["date"]
            feat["match_id"] = row.get("match_id")
            features_list.append(feat)
        else:
            skipped_min_matches += 1

        # Update trackers AFTER feature extraction
        elo.update(team1, team2, team1_win)
        stats.record_match(team1, team2, team1_win, date_str, tier=tier)

    logger.info(f"Skipped {skipped_min_matches} matches (teams with <{MIN_MATCHES} prior matches)")
    logger.info(f"Feature matrix: {len(features_list)} rows")

    if not features_list:
        raise ValueError("No features generated. Check data quality.")

    df_features = pd.DataFrame(features_list)

    # Fill NaN defaults
    fill_defaults = {
        "h2h_wr": 0.5,
        "team1_tier_wr": 0.5,
        "team2_tier_wr": 0.5,
        "team1_days_since_last": 30,
        "team2_days_since_last": 30,
        "tier": 4,
        "log_prizepool": 0.0,
    }
    for col, default in fill_defaults.items():
        if col in df_features.columns:
            df_features[col] = df_features[col].fillna(default)

    # Fill any remaining NaN in numeric columns
    numeric_cols = df_features.select_dtypes(include=[np.number]).columns
    df_features[numeric_cols] = df_features[numeric_cols].fillna(0)

    # Save outputs
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(ELO_SAVE_PATH), exist_ok=True)

    df_features.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"Saved feature matrix ({len(df_features)} rows, {len(df_features.columns)} cols) to {OUTPUT_PATH}")

    elo.save(ELO_SAVE_PATH)
    stats.save(STATS_SAVE_PATH)

    # Summary
    logger.info("=" * 60)
    logger.info("FEATURE BUILD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total clean matches: {len(df_features) + skipped_min_matches}")
    logger.info(f"Skipped (min matches): {skipped_min_matches}")
    logger.info(f"Feature matrix rows: {len(df_features)}")
    logger.info(f"Feature columns: {[c for c in df_features.columns if c not in ['team1_win', 'date', 'match_id']]}")
    logger.info(f"Target distribution: {df_features['team1_win'].value_counts().to_dict()}")
    logger.info(f"Teams tracked (Elo): {len(elo.ratings)}")
    logger.info(f"Teams tracked (stats): {len(stats.history)}")
    logger.info("=" * 60)


def extract_features(elo, stats, team1, team2, date_str, tier, prizepool, t_type):
    """Extract all features for a single match using only prior data."""
    feat = {}

    # Elo features
    feat["team1_elo"] = elo.get_rating(team1)
    feat["team2_elo"] = elo.get_rating(team2)
    feat["elo_diff"] = feat["team1_elo"] - feat["team2_elo"]

    # Win rate features
    feat["team1_wr_all"] = stats.get_win_rate(team1) or 0.5
    feat["team2_wr_all"] = stats.get_win_rate(team2) or 0.5
    feat["wr_diff_all"] = feat["team1_wr_all"] - feat["team2_wr_all"]

    feat["team1_wr_20"] = stats.get_win_rate(team1, last_n=20) or 0.5
    feat["team2_wr_20"] = stats.get_win_rate(team2, last_n=20) or 0.5

    feat["team1_wr_50"] = stats.get_win_rate(team1, last_n=50) or 0.5
    feat["team2_wr_50"] = stats.get_win_rate(team2, last_n=50) or 0.5

    # Form (last 10 matches)
    feat["team1_form"] = stats.get_recent_form(team1, last_n=10) or 0.5
    feat["team2_form"] = stats.get_recent_form(team2, last_n=10) or 0.5
    feat["form_diff"] = feat["team1_form"] - feat["team2_form"]

    # Streak
    feat["team1_streak"] = stats.get_win_streak(team1)
    feat["team2_streak"] = stats.get_win_streak(team2)
    feat["streak_diff"] = feat["team1_streak"] - feat["team2_streak"]

    # H2H
    feat["h2h_wr"] = stats.get_h2h_win_rate(team1, team2)

    # Tier-specific
    if tier is not None and not (isinstance(tier, float) and math.isnan(tier)):
        feat["team1_tier_wr"] = stats.get_tier_win_rate(team1, tier)
        feat["team2_tier_wr"] = stats.get_tier_win_rate(team2, tier)
    else:
        feat["team1_tier_wr"] = None
        feat["team2_tier_wr"] = None

    # Activity
    feat["team1_days_since_last"] = stats.get_days_since_last_match(team1, date_str)
    feat["team2_days_since_last"] = stats.get_days_since_last_match(team2, date_str)

    # Tournament context
    try:
        feat["tier"] = int(tier) if tier is not None and not (isinstance(tier, float) and math.isnan(tier)) else None
    except (ValueError, TypeError):
        feat["tier"] = None

    try:
        pp = float(prizepool) if prizepool is not None and not (isinstance(prizepool, float) and math.isnan(prizepool)) else 0.0
        feat["log_prizepool"] = math.log1p(pp)
    except (ValueError, TypeError):
        feat["log_prizepool"] = 0.0

    feat["is_online"] = 1 if t_type == "Online" else 0
    feat["is_offline"] = 1 if t_type == "Offline" else 0
    feat["is_hybrid"] = 1 if t_type == "Online/Offline" else 0

    return feat


if __name__ == "__main__":
    build_feature_matrix()
