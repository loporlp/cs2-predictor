import json
from collections import defaultdict

from src.utils.logger import get_logger

logger = get_logger("cs2predictor.features.team_stats")


class TeamStatsTracker:
    """Tracks rolling statistics per team: win rates, form, streaks, H2H, tier performance."""

    def __init__(self):
        # Per-team match history: [(date_str, won_bool)]
        self.history = defaultdict(list)
        # Per-team tier history: {team: {tier: [(date_str, won_bool)]}}
        self.tier_history = defaultdict(lambda: defaultdict(list))
        # H2H records: {(teamA, teamB): {teamA: wins, teamB: wins}}
        self.h2h = defaultdict(lambda: defaultdict(int))
        # Last match date per team
        self.last_match_date = {}

    def record_match(self, team1, team2, team1_win, date_str, tier=None):
        """Record a match result for both teams."""
        t1_won = bool(team1_win)
        t2_won = not t1_won

        self.history[team1].append((date_str, t1_won))
        self.history[team2].append((date_str, t2_won))

        if tier is not None:
            self.tier_history[team1][str(tier)].append((date_str, t1_won))
            self.tier_history[team2][str(tier)].append((date_str, t2_won))

        # H2H - use sorted tuple for consistent key
        h2h_key = tuple(sorted([team1, team2]))
        winner = team1 if t1_won else team2
        self.h2h[h2h_key][winner] += 1

        self.last_match_date[team1] = date_str
        self.last_match_date[team2] = date_str

    def get_win_rate(self, team, last_n=None):
        """Overall win rate, optionally over last N matches."""
        hist = self.history.get(team)
        if not hist:
            return None
        if last_n is not None:
            hist = hist[-last_n:]
        wins = sum(1 for _, won in hist if won)
        return wins / len(hist)

    def get_recent_form(self, team, last_n=10):
        """Short-term momentum (win rate over last N matches)."""
        return self.get_win_rate(team, last_n=last_n)

    def get_win_streak(self, team):
        """Current streak: positive for wins, negative for losses."""
        hist = self.history.get(team)
        if not hist:
            return 0
        streak = 0
        last_result = hist[-1][1]
        for _, won in reversed(hist):
            if won == last_result:
                streak += 1
            else:
                break
        return streak if last_result else -streak

    def get_h2h_win_rate(self, team, opponent):
        """Head-to-head win rate for team against opponent."""
        h2h_key = tuple(sorted([team, opponent]))
        record = self.h2h.get(h2h_key)
        if not record:
            return None
        total = sum(record.values())
        if total == 0:
            return None
        return record.get(team, 0) / total

    def get_tier_win_rate(self, team, tier):
        """Win rate at a specific tournament tier."""
        tier_hist = self.tier_history.get(team, {}).get(str(tier))
        if not tier_hist:
            return None
        wins = sum(1 for _, won in tier_hist if won)
        return wins / len(tier_hist)

    def get_days_since_last_match(self, team, current_date):
        """Days since team's last match. Returns None if no history."""
        last = self.last_match_date.get(team)
        if not last:
            return None
        try:
            from datetime import datetime
            last_dt = datetime.fromisoformat(str(last).replace(" ", "T").split("T")[0])
            curr_dt = datetime.fromisoformat(str(current_date).replace(" ", "T").split("T")[0])
            return (curr_dt - last_dt).days
        except (ValueError, TypeError):
            return None

    def get_total_matches(self, team):
        """Total matches played by team."""
        return len(self.history.get(team, []))

    def save(self, filepath):
        """Save all stats to JSON."""
        data = {
            "history": {k: v for k, v in self.history.items()},
            "tier_history": {
                k: {tk: tv for tk, tv in v.items()}
                for k, v in self.tier_history.items()
            },
            "h2h": {
                "|".join(k): dict(v) for k, v in self.h2h.items()
            },
            "last_match_date": dict(self.last_match_date),
        }
        with open(filepath, "w") as f:
            json.dump(data, f)
        logger.info(f"Saved team stats for {len(self.history)} teams to {filepath}")

    @classmethod
    def load(cls, filepath):
        """Load stats from JSON."""
        with open(filepath, "r") as f:
            data = json.load(f)

        tracker = cls()
        for team, hist in data["history"].items():
            tracker.history[team] = [(d, w) for d, w in hist]
        for team, tiers in data["tier_history"].items():
            for tier, hist in tiers.items():
                tracker.tier_history[team][tier] = [(d, w) for d, w in hist]
        for key_str, record in data["h2h"].items():
            key = tuple(key_str.split("|"))
            tracker.h2h[key] = defaultdict(int, record)
        tracker.last_match_date = data["last_match_date"]

        logger.info(f"Loaded team stats for {len(tracker.history)} teams from {filepath}")
        return tracker
