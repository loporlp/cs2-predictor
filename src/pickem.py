"""CS2 Major Swiss Pick'em Simulator.

Runs Monte Carlo simulations of a CS2 Major-style Swiss bracket and produces
pick'em recommendations: best 3-0 team, best 0-3 team, and most likely
teams to advance.
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple

import numpy as np

from src.predict import build_feature_vector, load_artifacts
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.pickem")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Team:
    name: str
    seed: int
    wins: int = 0
    losses: int = 0
    opponents: set = field(default_factory=set)

    def is_done(self) -> bool:
        return self.wins == 3 or self.losses == 3

    def record(self) -> str:
        return f"{self.wins}-{self.losses}"

    def copy(self) -> "Team":
        return Team(self.name, self.seed, self.wins, self.losses, set(self.opponents))


# ---------------------------------------------------------------------------
# Matchup creation
# ---------------------------------------------------------------------------

def create_round1_matchups(teams: List[Team]) -> List[Tuple[Team, Team]]:
    """Pair top half vs bottom half by seed for round 1."""
    sorted_teams = sorted(teams, key=lambda t: t.seed)
    n = len(sorted_teams)
    if n % 2 != 0:
        raise ValueError(f"Team count must be even, got {n}")
    half = n // 2
    return [(sorted_teams[i], sorted_teams[i + half]) for i in range(half)]


def create_matchups(group: List[Team]) -> List[Tuple[Team, Team]]:
    """Within a W-L bucket, pair head-to-tail (best vs worst) with no-rematch shift."""
    sorted_group = sorted(group, key=lambda t: t.seed)
    available = list(sorted_group)
    matchups = []

    while len(available) >= 2:
        team_a = available.pop(0)
        # Scan from the bottom to find a non-rematch opponent
        paired = False
        for idx in range(len(available) - 1, -1, -1):
            team_b = available[idx]
            if team_b.name not in team_a.opponents:
                available.pop(idx)
                matchups.append((team_a, team_b))
                paired = True
                break

        if not paired:
            # All remaining opponents are rematches — pair with the bottom one anyway
            logger.debug(
                "No valid non-rematch opponent for %s; pairing with bottom of bucket.",
                team_a.name,
            )
            team_b = available.pop(-1)
            matchups.append((team_a, team_b))

    return matchups


def _create_next_round_matchups(active: List[Team]) -> List[Tuple[Team, Team]]:
    """Group active teams by (wins, losses) and produce matchups for the next round.

    Odd-sized buckets have their worst-seeded team carried down to the next bucket.
    Buckets are processed top-down (most wins first, fewest losses first).
    """
    # Build buckets
    buckets: Dict[Tuple[int, int], List[Team]] = {}
    for team in active:
        key = (team.wins, team.losses)
        buckets.setdefault(key, []).append(team)

    # Sort bucket keys: more wins first; within same wins, fewer losses first
    sorted_keys = sorted(buckets.keys(), key=lambda k: (-k[0], k[1]))

    matchups: List[Tuple[Team, Team]] = []
    carry: Optional[Team] = None

    for key in sorted_keys:
        group = buckets[key]
        if carry is not None:
            group = [carry] + group
            carry = None

        # Sort by seed within the group
        group.sort(key=lambda t: t.seed)

        if len(group) % 2 != 0:
            # Worst seed gets carried down
            carry = group.pop(-1)

        matchups.extend(create_matchups(group))

    if carry is not None:
        # Shouldn't happen in a valid tournament, but log just in case
        logger.debug("Leftover team with no bucket to carry into: %s", carry.name)

    return matchups


# ---------------------------------------------------------------------------
# Probability pre-computation
# ---------------------------------------------------------------------------

def precompute_pairwise_probs(
    team_names: List[str],
    tier: int,
    prizepool: float,
    t_type: str,
    elo,
    stats,
    model,
    scaler,
    actual_model_name: str,
) -> Dict[FrozenSet, Dict[str, float]]:
    """Pre-compute win probabilities for every pair of teams.

    Returns a dict keyed by frozenset({name_a, name_b}) mapping each name
    to its probability of winning that head-to-head.
    """
    pairwise: Dict[FrozenSet, Dict[str, float]] = {}
    n = len(team_names)

    for i in range(n):
        for j in range(i + 1, n):
            a, b = team_names[i], team_names[j]
            X = build_feature_vector(elo, stats, a, b, tier, prizepool, t_type)

            if actual_model_name == "logistic_regression":
                X_pred = scaler.transform(X)
            else:
                X_pred = X

            proba = model.predict_proba(X_pred)[0]
            p_a = float(proba[1])  # P(team1=a wins)
            p_b = float(proba[0])  # P(team2=b wins)

            pairwise[frozenset({a, b})] = {a: p_a, b: p_b}

    return pairwise


# ---------------------------------------------------------------------------
# Single simulation
# ---------------------------------------------------------------------------

def simulate_once(
    teams_template: List[Team],
    pairwise_probs: Dict[FrozenSet, Dict[str, float]],
    rng: np.random.Generator,
) -> Dict[str, str]:
    """Run one full Swiss tournament simulation.

    Returns a dict mapping team name → final record string (e.g. "3-1").
    """
    # Deep-copy so the template stays clean
    teams: Dict[str, Team] = {t.name: t.copy() for t in teams_template}
    active: List[Team] = list(teams.values())

    final_records: Dict[str, str] = {}

    # Round 1
    matchups = create_round1_matchups(active)

    while matchups:
        next_active: List[Team] = []

        for team_a, team_b in matchups:
            key = frozenset({team_a.name, team_b.name})
            p_a = pairwise_probs[key][team_a.name]

            if rng.random() < p_a:
                winner, loser = team_a, team_b
            else:
                winner, loser = team_b, team_a

            winner.wins += 1
            loser.losses += 1
            winner.opponents.add(loser.name)
            loser.opponents.add(winner.name)

            for team in (winner, loser):
                if team.is_done():
                    final_records[team.name] = team.record()
                else:
                    next_active.append(team)

        active = next_active
        if not active:
            break

        matchups = _create_next_round_matchups(active)

    return final_records


# ---------------------------------------------------------------------------
# Aggregation and main simulation runner
# ---------------------------------------------------------------------------

def run_pickem(
    lineup: List[Tuple[int, str]],   # list of (seed, name)
    tier: int = 1,
    prizepool: float = 1_250_000,
    t_type: str = "Offline",
    n_simulations: int = 10_000,
    model_name: str = "best",
    seed: Optional[int] = None,
) -> dict:
    """Run Monte Carlo pick'em simulation and return structured results."""

    logger.info(
        "Loading artifacts for model '%s'…", model_name
    )
    elo, stats, model, scaler, metadata, actual_model_name = load_artifacts(model_name)
    logger.info("Model loaded: %s", actual_model_name)

    team_names = [name for _, name in sorted(lineup, key=lambda x: x[0])]

    # Warn about teams with little or no history
    warnings: List[str] = []
    for name in team_names:
        count = stats.get_total_matches(name)
        if count == 0:
            warnings.append(f"'{name}' has no match history — using default ratings.")
        elif count < 10:
            warnings.append(f"'{name}' has only {count} matches — predictions may be unreliable.")

    # Build template
    teams_template: List[Team] = [
        Team(name=name, seed=seed_val)
        for seed_val, name in sorted(lineup, key=lambda x: x[0])
    ]

    logger.info(
        "Pre-computing pairwise probabilities for %d teams (%d pairs)…",
        len(team_names),
        len(team_names) * (len(team_names) - 1) // 2,
    )
    pairwise_probs = precompute_pairwise_probs(
        team_names, tier, prizepool, t_type,
        elo, stats, model, scaler, actual_model_name,
    )

    rng = np.random.default_rng(seed)

    # Accumulators: {name: {record: count, "wins_sum": int, "losses_sum": int}}
    accum: Dict[str, Dict] = {
        name: {"wins_sum": 0, "losses_sum": 0, "records": {}}
        for name in team_names
    }

    logger.info("Running %d simulations…", n_simulations)
    for _ in range(n_simulations):
        result = simulate_once(teams_template, pairwise_probs, rng)
        for name, rec in result.items():
            w, l = map(int, rec.split("-"))
            accum[name]["wins_sum"] += w
            accum[name]["losses_sum"] += l
            accum[name]["records"][rec] = accum[name]["records"].get(rec, 0) + 1

    def pct(count: int) -> float:
        return round(count / n_simulations * 100, 1)

    teams_output = []
    seed_map = {name: s for s, name in lineup}
    for name in team_names:
        a = accum[name]
        records = a["records"]
        advance_count = records.get("3-0", 0) + records.get("3-1", 0) + records.get("3-2", 0)
        elim_count = records.get("0-3", 0) + records.get("1-3", 0) + records.get("2-3", 0)

        teams_output.append({
            "name": name,
            "seed": seed_map[name],
            "advance_pct": pct(advance_count),
            "eliminate_pct": pct(elim_count),
            "record_3_0_pct": pct(records.get("3-0", 0)),
            "record_3_1_pct": pct(records.get("3-1", 0)),
            "record_3_2_pct": pct(records.get("3-2", 0)),
            "record_2_3_pct": pct(records.get("2-3", 0)),
            "record_1_3_pct": pct(records.get("1-3", 0)),
            "record_0_3_pct": pct(records.get("0-3", 0)),
            "avg_wins": round(a["wins_sum"] / n_simulations, 2),
            "avg_losses": round(a["losses_sum"] / n_simulations, 2),
        })

    return {
        "metadata": {
            "tier": tier,
            "prizepool": prizepool,
            "t_type": t_type,
            "n_simulations": n_simulations,
            "model": actual_model_name,
            "seed": seed,
        },
        "warnings": warnings,
        "teams": teams_output,
    }


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_pickem_results(result: dict) -> None:
    """Print a human-readable pick'em results table."""
    meta = result["metadata"]
    teams = result["teams"]
    warnings = result.get("warnings", [])

    print()
    print("=" * 97)
    print("  CS2 SWISS PICK'EM SIMULATOR")
    print(
        f"  Tier {meta['tier']}  |  ${meta['prizepool']:,.0f}  |  {meta['t_type']}  |  "
        f"{meta['n_simulations']:,} simulations  |  Model: {meta['model']}"
    )
    print("=" * 97)

    if warnings:
        print()
        for w in warnings:
            print(f"  WARNING: {w}")

    # Table header
    print()
    header = (
        f"  {'Team':<25} {'Seed':>4}  {'Advance%':>8}  {'Elim%':>6}  "
        f"{'3-0%':>5}  {'3-1%':>5}  {'3-2%':>5}  {'2-3%':>5}  {'1-3%':>5}  {'0-3%':>5}  {'Avg W-L':>7}"
    )
    print(header)
    print("  " + "-" * 93)

    sorted_teams = sorted(teams, key=lambda t: t["advance_pct"], reverse=True)

    for t in sorted_teams:
        avg_wl = f"{t['avg_wins']:.2f}-{t['avg_losses']:.2f}"
        print(
            f"  {t['name']:<25} {t['seed']:>4}  {t['advance_pct']:>8.1f}  {t['eliminate_pct']:>6.1f}  "
            f"{t['record_3_0_pct']:>5.1f}  {t['record_3_1_pct']:>5.1f}  {t['record_3_2_pct']:>5.1f}  "
            f"{t['record_2_3_pct']:>5.1f}  {t['record_1_3_pct']:>5.1f}  {t['record_0_3_pct']:>5.1f}  {avg_wl:>7}"
        )

    # Recommendations
    print()
    print("=" * 97)
    print("  PICK'EM RECOMMENDATIONS")
    print("=" * 97)

    best_3_0 = max(teams, key=lambda t: t["record_3_0_pct"])
    best_0_3 = max(teams, key=lambda t: t["record_0_3_pct"])
    top_advance = sorted(teams, key=lambda t: t["advance_pct"], reverse=True)[:3]
    top_elim = sorted(teams, key=lambda t: t["eliminate_pct"], reverse=True)[:3]

    print()
    print(
        f"  Best 3-0 pick  : {best_3_0['name']}  "
        f"({best_3_0['record_3_0_pct']:.1f}% chance)"
    )
    print(
        f"  Best 0-3 pick  : {best_0_3['name']}  "
        f"({best_0_3['record_0_3_pct']:.1f}% chance)"
    )
    print()
    print("  Top teams to advance:")
    for t in top_advance:
        print(f"    {t['name']:<25}  {t['advance_pct']:.1f}% advance")
    print()
    print("  Most likely eliminated:")
    for t in top_elim:
        print(f"    {t['name']:<25}  {t['eliminate_pct']:.1f}% eliminated")

    print()
    print("=" * 97)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_lineup_arg(raw: List[str]) -> List[Tuple[int, str]]:
    """Parse 'SEED:Team Name' strings into (seed, name) tuples."""
    lineup = []
    for entry in raw:
        colon_idx = entry.index(":")
        seed_str = entry[:colon_idx]
        name = entry[colon_idx + 1:]
        try:
            seed = int(seed_str)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid seed in '{entry}': seed must be an integer."
            )
        lineup.append((seed, name))
    return lineup


def _validate_lineup(lineup: List[Tuple[int, str]]) -> None:
    """Validate lineup for even count, unique seeds, unique names."""
    if len(lineup) % 2 != 0:
        raise ValueError(f"Lineup must have an even number of teams, got {len(lineup)}.")

    seeds = [s for s, _ in lineup]
    if len(seeds) != len(set(seeds)):
        raise ValueError("Duplicate seeds found in lineup.")

    names = [n for _, n in lineup]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate team names found in lineup.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CS2 Major Swiss Pick'em Simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python -m src.pickem \\
    --lineup "1:FaZe Clan" "2:Natus Vincere" "3:Team Spirit" "4:G2 Esports" \\
             "5:MOUZ" "6:Team Vitality" "7:Heroic" "8:FURIA" \\
             "9:ENCE" "10:Liquid" "11:BIG" "12:Cloud9" \\
             "13:Complexity" "14:GamerLegion" "15:Fluxo" "16:9z Team" \\
    --tier 1 --prizepool 1250000 --simulations 10000
        """,
    )

    parser.add_argument(
        "--lineup",
        nargs="+",
        required=True,
        metavar="SEED:Name",
        help="Team lineup in 'SEED:Team Name' format (e.g. '1:FaZe Clan')",
    )
    parser.add_argument(
        "--tier", type=int, default=1,
        help="Tournament tier 1-4 (default: 1)",
    )
    parser.add_argument(
        "--prizepool", type=float, default=1_250_000,
        help="Prize pool in USD (default: 1250000)",
    )
    parser.add_argument(
        "--type", dest="t_type", default="Offline",
        choices=["Online", "Offline", "Online/Offline"],
        help="Tournament type (default: Offline)",
    )
    parser.add_argument(
        "--simulations", type=int, default=10_000,
        help="Number of Monte Carlo simulations (default: 10000)",
    )
    parser.add_argument(
        "--model", default="best",
        choices=["best", "logistic_regression", "random_forest", "xgboost", "gradient_boosting"],
        help="Model to use (default: best)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output JSON instead of a formatted table",
    )

    args = parser.parse_args()

    try:
        lineup = _parse_lineup_arg(args.lineup)
        _validate_lineup(lineup)
    except (ValueError, argparse.ArgumentTypeError) as exc:
        parser.error(str(exc))

    result = run_pickem(
        lineup=lineup,
        tier=args.tier,
        prizepool=args.prizepool,
        t_type=args.t_type,
        n_simulations=args.simulations,
        model_name=args.model,
        seed=args.seed,
    )

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_pickem_results(result)


if __name__ == "__main__":
    main()
