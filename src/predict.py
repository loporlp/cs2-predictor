import argparse
import json
import os
import sys

import joblib

from src.features.elo import EloRatingSystem
from src.features.team_stats import TeamStatsTracker
from src.model.features import FEATURE_COLS
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.predict")

MODELS_DIR = "data/models"


def load_artifacts(model_name="best"):
    """Load Elo ratings, team stats, the model bundle, and training metadata.

    Returns:
        (elo, stats, predictor, metadata, actual_model_name) where `predictor`
        is a src.model.predictor.Predictor (`predictor.predict_proba(X)` gives
        P(team1 wins), order-symmetric and calibrated).
    """
    from src.model.predictor import Predictor

    elo = EloRatingSystem.load(os.path.join(MODELS_DIR, "elo_ratings.json"))
    stats = TeamStatsTracker.load(os.path.join(MODELS_DIR, "team_stats.json"))

    filename = "best_model.joblib" if model_name == "best" else f"model_{model_name}.joblib"
    predictor = joblib.load(os.path.join(MODELS_DIR, filename))
    if not isinstance(predictor, Predictor):
        raise RuntimeError(
            f"{filename} was saved in the old (pre-Predictor) format. "
            "Retrain with `python -m src.model.train` or the UI's Retrain panel."
        )

    with open(os.path.join(MODELS_DIR, "training_metadata.json")) as f:
        metadata = json.load(f)

    actual_model_name = metadata["best_model"] if model_name == "best" else model_name
    return elo, stats, predictor, metadata, actual_model_name


def build_feature_vector(elo, stats, team1, team2, tier, prizepool, t_type, date_str=None):
    """Build a single (1, n_features) vector for prediction.

    Uses the exact same extraction and default-filling code as training
    (src.features.build_features) so there is no train/serve skew.
    """
    from datetime import datetime

    import pandas as pd

    from src.features.build_features import extract_features, fill_feature_defaults

    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    feat = extract_features(elo, stats, team1, team2, date_str, tier, prizepool, t_type)
    df = fill_feature_defaults(pd.DataFrame([feat]))
    return df[FEATURE_COLS].to_numpy(dtype=float)


def predict(team1, team2, tier=2, prizepool=50000, t_type="Offline",
            model_name="best", output_json=False):
    """Run a prediction and output results."""
    elo, stats, predictor, metadata, actual_model_name = load_artifacts(model_name)

    # Warn about unknown/limited teams
    warnings = []
    for team in [team1, team2]:
        count = stats.get_total_matches(team)
        if count == 0:
            warnings.append(f"WARNING: '{team}' has no match history. Using default ratings.")
        elif count < 10:
            warnings.append(f"WARNING: '{team}' has only {count} matches. Prediction may be unreliable.")

    X = build_feature_vector(elo, stats, team1, team2, tier, prizepool, t_type)
    team1_prob = float(predictor.predict_proba(X)[0])
    team2_prob = 1.0 - team1_prob

    winner = team1 if team1_prob > 0.5 else team2
    confidence = max(team1_prob, team2_prob)

    t1_matches = stats.get_total_matches(team1)
    t2_matches = stats.get_total_matches(team2)
    h2h = stats.get_h2h_win_rate(team1, team2)

    result = {
        "team1": team1,
        "team2": team2,
        "team1_probability": round(team1_prob * 100, 1),
        "team2_probability": round(team2_prob * 100, 1),
        "predicted_winner": winner,
        "confidence": round(confidence * 100, 1),
        "context": {
            "team1_elo": round(elo.get_rating(team1), 1),
            "team2_elo": round(elo.get_rating(team2), 1),
            "team1_matches": t1_matches,
            "team2_matches": t2_matches,
            "h2h_win_rate": f"{h2h * 100:.1f}%" if h2h is not None else "N/A",
            "model": actual_model_name,
            "tournament": f"Tier {tier}, ${prizepool:,}, {t_type}",
        },
        "warnings": warnings,
    }

    if output_json:
        print(json.dumps(result, indent=2))
    else:
        print_prediction(result)

    return result


def print_prediction(result):
    """Print formatted prediction output."""
    t1 = result["team1"]
    t2 = result["team2"]
    ctx = result["context"]

    print()
    print("=" * 60)
    print("  CS2 MATCH PREDICTION")
    print("=" * 60)

    for w in result["warnings"]:
        print(f"  {w}")
    if result["warnings"]:
        print()

    print(f"  {t1}  vs  {t2}")
    print(f"  {'─' * 40}")
    print(f"  {t1}: {result['team1_probability']}% win probability")
    print(f"  {t2}: {result['team2_probability']}% win probability")
    print()
    print(f"  Predicted winner: {result['predicted_winner']} ({result['confidence']}% confidence)")
    print()
    print(f"  Context:")
    print(f"    Elo: {t1} ({ctx['team1_elo']}) vs {t2} ({ctx['team2_elo']})")
    print(f"    Matches played: {ctx['team1_matches']} vs {ctx['team2_matches']}")
    print(f"    H2H win rate ({t1}): {ctx['h2h_win_rate']}")
    print(f"    Model: {ctx['model']}")
    print(f"    Tournament: {ctx['tournament']}")
    print("=" * 60)
    print()


def list_teams(elo, stats, top_n=50):
    """List teams sorted by Elo rating."""
    teams = sorted(elo.ratings.items(), key=lambda x: x[1], reverse=True)
    print(f"\nTop {min(top_n, len(teams))} teams by Elo rating:")
    print(f"{'Rank':>4}  {'Team':<35} {'Elo':>7}  {'Matches':>7}  {'Win Rate':>8}")
    print("-" * 70)
    for i, (team, rating) in enumerate(teams[:top_n], 1):
        matches = stats.get_total_matches(team)
        wr = stats.get_win_rate(team)
        wr_str = f"{wr * 100:.1f}%" if wr is not None else "N/A"
        print(f"{i:>4}  {team:<35} {rating:>7.1f}  {matches:>7}  {wr_str:>8}")
    print()


def main():
    parser = argparse.ArgumentParser(description="CS2 Match Outcome Prediction")
    parser.add_argument("--team1", type=str, help="Team 1 name")
    parser.add_argument("--team2", type=str, help="Team 2 name")
    parser.add_argument("--tier", type=int, default=2, help="Tournament tier 1-4 (default: 2)")
    parser.add_argument("--prizepool", type=float, default=50000, help="Prize pool in USD (default: 50000)")
    parser.add_argument("--type", type=str, default="Offline", choices=["Online", "Offline", "Online/Offline"],
                        help="Tournament type (default: Offline)")
    parser.add_argument("--model", type=str, default="best",
                        choices=["best", "logistic_regression", "random_forest", "xgboost", "gradient_boosting"],
                        help="Model to use (default: best)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--list-teams", action="store_true", help="List known teams by Elo rating")

    args = parser.parse_args()

    if args.list_teams:
        elo, stats, _, _, _, _ = load_artifacts()
        list_teams(elo, stats)
        return

    if not args.team1 or not args.team2:
        parser.error("--team1 and --team2 are required (or use --list-teams)")

    predict(
        team1=args.team1,
        team2=args.team2,
        tier=args.tier,
        prizepool=args.prizepool,
        t_type=args.type,
        model_name=args.model,
        output_json=args.json,
    )


if __name__ == "__main__":
    main()
