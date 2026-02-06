import json
import math
from collections import defaultdict

from src.utils.logger import get_logger

logger = get_logger("cs2predictor.features.elo")


class EloRatingSystem:
    """Elo rating system for CS2 teams with adaptive K-factor."""

    def __init__(self, k_factor=32.0, default_rating=1500.0):
        self.k_factor = k_factor
        self.default_rating = default_rating
        self.ratings = defaultdict(lambda: self.default_rating)
        self.match_counts = defaultdict(int)

    def expected_score(self, rating_a, rating_b):
        """Standard Elo expected score formula."""
        return 1.0 / (1.0 + math.pow(10, (rating_b - rating_a) / 400.0))

    def _get_k(self, team):
        """Adaptive K-factor: higher for teams with fewer matches."""
        if self.match_counts[team] < 30:
            return 48.0
        return self.k_factor

    def update(self, team1_name, team2_name, team1_win):
        """Update ratings after a match.

        Args:
            team1_name: Name of team 1
            team2_name: Name of team 2
            team1_win: 1 if team1 won, 0 if team2 won
        """
        r1 = self.ratings[team1_name]
        r2 = self.ratings[team2_name]

        e1 = self.expected_score(r1, r2)
        e2 = 1.0 - e1

        s1 = float(team1_win)
        s2 = 1.0 - s1

        k1 = self._get_k(team1_name)
        k2 = self._get_k(team2_name)

        self.ratings[team1_name] = r1 + k1 * (s1 - e1)
        self.ratings[team2_name] = r2 + k2 * (s2 - e2)

        self.match_counts[team1_name] += 1
        self.match_counts[team2_name] += 1

    def get_rating(self, team_name):
        """Get current Elo rating for a team."""
        return self.ratings[team_name]

    def get_match_count(self, team_name):
        """Get total match count for a team."""
        return self.match_counts[team_name]

    def save(self, filepath):
        """Save ratings and match counts to JSON."""
        data = {
            "ratings": dict(self.ratings),
            "match_counts": dict(self.match_counts),
            "k_factor": self.k_factor,
            "default_rating": self.default_rating,
        }
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved Elo ratings for {len(self.ratings)} teams to {filepath}")

    @classmethod
    def load(cls, filepath):
        """Load ratings and match counts from JSON."""
        with open(filepath, "r") as f:
            data = json.load(f)

        elo = cls(
            k_factor=data.get("k_factor", 32.0),
            default_rating=data.get("default_rating", 1500.0),
        )
        for team, rating in data["ratings"].items():
            elo.ratings[team] = rating
        for team, count in data["match_counts"].items():
            elo.match_counts[team] = count

        logger.info(f"Loaded Elo ratings for {len(elo.ratings)} teams from {filepath}")
        return elo
