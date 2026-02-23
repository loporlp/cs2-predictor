"""Web UI for the CS2 Pick'em Simulator."""

import logging
import os

from flask import Flask, jsonify, render_template, request

from src.pickem import run_pickem
from src.predict import load_artifacts
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.ui")

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))

# Silence Flask's default request logger so it doesn't pollute stdout
logging.getLogger("werkzeug").setLevel(logging.WARNING)

_artifacts = None


def _get_artifacts():
    global _artifacts
    if _artifacts is None:
        logger.info("Loading model artifacts…")
        _artifacts = load_artifacts("best")
        logger.info("Artifacts loaded.")
    return _artifacts


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/teams")
def teams():
    """Return all known teams sorted by Elo rating."""
    elo, stats, _, _, _, _ = _get_artifacts()
    teams_list = sorted(
        [
            {
                "name": name,
                "elo": round(rating, 1),
                "matches": stats.get_total_matches(name),
            }
            for name, rating in elo.ratings.items()
        ],
        key=lambda t: t["elo"],
        reverse=True,
    )
    return jsonify(teams_list)


@app.route("/api/simulate", methods=["POST"])
def simulate():
    """Run a Swiss simulation and return results as JSON."""
    data = request.get_json(force=True)

    try:
        lineup = [(int(e["seed"]), e["name"]) for e in data["lineup"]]
        result = run_pickem(
            lineup=lineup,
            tier=int(data.get("tier", 1)),
            prizepool=float(data.get("prizepool", 1_250_000)),
            t_type=data.get("t_type", "Offline"),
            n_simulations=int(data.get("n_simulations", 10_000)),
            model_name="best",
            seed=data.get("seed") or None,
        )
        return jsonify({"ok": True, "result": result})
    except Exception as exc:
        logger.exception("Simulation error")
        return jsonify({"ok": False, "error": str(exc)}), 400


def main():
    import webbrowser
    import threading

    port = 5000
    url = f"http://127.0.0.1:{port}"

    # Pre-load artifacts before the server starts
    _get_artifacts()

    def open_browser():
        webbrowser.open(url)

    threading.Timer(0.5, open_browser).start()
    print(f"\n  CS2 Pick'em UI running at {url}\n  Press Ctrl+C to stop.\n")
    app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    main()
