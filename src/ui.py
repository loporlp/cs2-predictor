"""Web UI for the CS2 Pick'em Simulator."""

import logging
import os
import threading

from flask import Flask, jsonify, render_template, request

from src.pickem import run_pickem
from src.predict import load_artifacts, build_feature_vector
from src.utils.logger import get_logger

logger = get_logger("cs2predictor.ui")

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))

# Silence Flask's default request logger
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Artifact cache
# ---------------------------------------------------------------------------

_artifacts = None
_artifacts_lock = threading.Lock()


def _get_artifacts():
    global _artifacts
    with _artifacts_lock:
        if _artifacts is None:
            logger.info("Loading model artifacts…")
            _artifacts = load_artifacts("best")
            logger.info("Artifacts loaded.")
        return _artifacts


def _clear_artifacts():
    global _artifacts
    with _artifacts_lock:
        _artifacts = None


# ---------------------------------------------------------------------------
# Retrain state
# ---------------------------------------------------------------------------

_retrain_lock = threading.Lock()
_retrain_state = {
    "status": "idle",   # idle | running | done | error
    "logs": [],
    "metrics": None,
    "error": None,
}


class _StateLogHandler(logging.Handler):
    """Appends formatted log records into a job state's log list."""

    def __init__(self, state, lock):
        super().__init__()
        self._state = state
        self._lock = lock

    def emit(self, record):
        msg = self.format(record)
        with self._lock:
            self._state["logs"].append(msg)


def _run_retrain(cutoff_date, train_start_date):
    """Background thread: rebuild features then train."""
    handler = _StateLogHandler(_retrain_state, _retrain_lock)
    handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    root = logging.getLogger("cs2predictor")
    root.addHandler(handler)

    try:
        from src.features.build_features import build_feature_matrix
        from src.model.train import train_pipeline

        build_feature_matrix(cutoff_date=cutoff_date, train_start_date=train_start_date)
        metadata = train_pipeline(cutoff_date=cutoff_date, train_start_date=train_start_date)

        _clear_artifacts()   # force reload on next prediction

        with _retrain_lock:
            _retrain_state["status"] = "done"
            _retrain_state["metrics"] = metadata

    except Exception as exc:
        logger.exception("Retraining failed")
        with _retrain_lock:
            _retrain_state["status"] = "error"
            _retrain_state["error"] = str(exc)
    finally:
        root.removeHandler(handler)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/teams")
def teams():
    """Return all known teams sorted by Elo rating."""
    elo, stats, _, _, _ = _get_artifacts()
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


@app.route("/api/model-info")
def model_info():
    """Return training metadata for all available models."""
    import json as _json
    meta_path = os.path.join("data/models", "training_metadata.json")
    try:
        with open(meta_path) as f:
            return jsonify({"ok": True, "metadata": _json.load(f)})
    except FileNotFoundError:
        return jsonify({"ok": False, "error": "training_metadata.json not found"}), 404


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
            model_name=data.get("model_name", "best"),
            seed=data.get("seed") or None,
        )
        return jsonify({"ok": True, "result": result})
    except Exception as exc:
        logger.exception("Simulation error")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/retrain", methods=["POST"])
def retrain():
    """Start a retraining job in the background."""
    with _retrain_lock:
        if _retrain_state["status"] == "running":
            return jsonify({"ok": False, "error": "Retraining already in progress."}), 409

        data = request.get_json(force=True)
        cutoff_date = data.get("cutoff_date") or None         # YYYY-MM-DD or None
        train_start_date = data.get("train_start_date") or None  # YYYY-MM-DD or None

        _retrain_state.update({
            "status": "running",
            "logs": [],
            "metrics": None,
            "error": None,
        })

    thread = threading.Thread(target=_run_retrain, args=(cutoff_date, train_start_date), daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.route("/api/retrain/status")
def retrain_status():
    """Return current retrain job state (for polling)."""
    with _retrain_lock:
        return jsonify(dict(_retrain_state))


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

@app.route("/api/predict", methods=["POST"])
def predict_match():
    """Predict a single match outcome."""
    data = request.get_json(force=True)

    try:
        team1 = data["team1"]
        team2 = data["team2"]
        tier = int(data.get("tier", 2))
        prizepool = float(data.get("prizepool", 50_000))
        t_type = data.get("t_type", "Offline")
        model_name = data.get("model_name", "best")

        elo, stats, predictor, metadata, actual_model_name = load_artifacts(model_name)

        warnings = []
        for team in [team1, team2]:
            count = stats.get_total_matches(team)
            if count == 0:
                warnings.append(f"'{team}' has no match history — using default ratings.")
            elif count < 10:
                warnings.append(f"'{team}' has only {count} matches — prediction may be unreliable.")

        X = build_feature_vector(elo, stats, team1, team2, tier, prizepool, t_type)
        team1_prob = float(predictor.predict_proba(X)[0])
        team2_prob = 1.0 - team1_prob

        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        h2h = stats.get_h2h_win_rate(team1, team2)

        result = {
            "team1": team1,
            "team2": team2,
            "team1_probability": round(team1_prob * 100, 1),
            "team2_probability": round(team2_prob * 100, 1),
            "predicted_winner": team1 if team1_prob > 0.5 else team2,
            "confidence": round(max(team1_prob, team2_prob) * 100, 1),
            "context": {
                "team1_elo": round(elo.get_rating(team1), 1),
                "team2_elo": round(elo.get_rating(team2), 1),
                "team1_matches": stats.get_total_matches(team1),
                "team2_matches": stats.get_total_matches(team2),
                "h2h_win_rate": f"{h2h * 100:.1f}%" if h2h is not None else "N/A",
                "model": actual_model_name,
            },
            "warnings": warnings,
        }
        return jsonify({"ok": True, "result": result})
    except Exception as exc:
        logger.exception("Prediction error")
        return jsonify({"ok": False, "error": str(exc)}), 400


# ---------------------------------------------------------------------------
# Data fetch pipeline
# ---------------------------------------------------------------------------

_fetch_lock = threading.Lock()
_fetch_state = {
    "status": "idle",   # idle | running | done | error
    "logs": [],
    "error": None,
}


def _run_fetch_data(operation, **kwargs):
    """Background thread: run tournament or match fetch pipeline."""
    handler = _StateLogHandler(_fetch_state, _fetch_lock)
    handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    root = logging.getLogger("cs2predictor")
    root.addHandler(handler)

    try:
        if operation == "tournaments":
            from src.pipelines.build_tournaments import build_tournament_table
            build_tournament_table(
                start_date=kwargs.get("start_date", "2023-10-15"),
                end_date=kwargs.get("end_date"),
            )
        elif operation == "matches":
            from src.pipelines.build_matches import build_match_table
            build_match_table(
                incremental=kwargs.get("incremental", False),
                tournament_start_date=kwargs.get("tournament_start_date"),
            )
        else:
            raise ValueError(f"Unknown operation: {operation}")

        with _fetch_lock:
            _fetch_state["status"] = "done"

    except Exception as exc:
        logger.exception("Data fetch failed")
        with _fetch_lock:
            _fetch_state["status"] = "error"
            _fetch_state["error"] = str(exc)
    finally:
        root.removeHandler(handler)


@app.route("/api/fetch-data", methods=["POST"])
def fetch_data():
    """Start a data fetch pipeline job in the background."""
    with _fetch_lock:
        if _fetch_state["status"] == "running":
            return jsonify({"ok": False, "error": "A fetch job is already in progress."}), 409

        data = request.get_json(force=True)
        operation = data.get("operation")
        if operation not in ("tournaments", "matches"):
            return jsonify({"ok": False, "error": "operation must be 'tournaments' or 'matches'"}), 400

        _fetch_state.update({
            "status": "running",
            "logs": [],
            "error": None,
        })

    kwargs = {}
    if operation == "tournaments":
        from datetime import datetime
        kwargs["start_date"] = data.get("start_date") or "2023-10-15"
        kwargs["end_date"] = data.get("end_date") or datetime.now().strftime("%Y-%m-%d")
    elif operation == "matches":
        kwargs["incremental"] = bool(data.get("incremental", False))
        kwargs["tournament_start_date"] = data.get("tournament_start_date") or None

    thread = threading.Thread(target=_run_fetch_data, args=(operation,), kwargs=kwargs, daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.route("/api/fetch-data/status")
def fetch_data_status():
    """Return current data fetch job state (for polling)."""
    with _fetch_lock:
        return jsonify(dict(_fetch_state))


# ---------------------------------------------------------------------------
# Recent matches browser
# ---------------------------------------------------------------------------

@app.route("/api/recent-matches")
def recent_matches():
    try:
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
        offset = max(int(request.args.get("offset", 0)), 0)
        team = request.args.get("team", "").strip()
        from src.db.database import get_recent_matches
        matches, total = get_recent_matches(limit=limit, offset=offset, team=team)
        return jsonify({"ok": True, "matches": matches, "total": total, "offset": offset, "limit": limit})
    except Exception as exc:
        logger.exception("Recent matches error")
        return jsonify({"ok": False, "error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Window search experiment
# ---------------------------------------------------------------------------

_experiment_lock = threading.Lock()
_experiment_state = {
    "status": "idle",   # idle | running | done | error
    "logs": [],
    "result": None,
    "error": None,
}


def _run_window_search(test_days):
    handler = _StateLogHandler(_experiment_state, _experiment_lock)
    handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    root = logging.getLogger("cs2predictor")
    root.addHandler(handler)

    try:
        from src.model.backtest import run_window_search
        result = run_window_search(test_days=test_days)

        with _experiment_lock:
            _experiment_state["status"] = "done"
            _experiment_state["result"] = result

    except Exception as exc:
        logger.exception("Window search failed")
        with _experiment_lock:
            _experiment_state["status"] = "error"
            _experiment_state["error"] = str(exc)
    finally:
        root.removeHandler(handler)


@app.route("/api/window-search", methods=["POST"])
def window_search():
    """Start a training window search experiment in the background."""
    with _experiment_lock:
        if _experiment_state["status"] == "running":
            return jsonify({"ok": False, "error": "Experiment already in progress."}), 409

        data = request.get_json(force=True)
        test_days = int(data.get("test_days", 90))

        _experiment_state.update({
            "status": "running",
            "logs": [],
            "result": None,
            "error": None,
        })

    thread = threading.Thread(target=_run_window_search, args=(test_days,), daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.route("/api/window-search/status")
def window_search_status():
    """Return current window search state (for polling)."""
    with _experiment_lock:
        return jsonify(dict(_experiment_state))


# ---------------------------------------------------------------------------
# Training run history
# ---------------------------------------------------------------------------

@app.route("/api/training-runs")
def training_runs():
    """List recorded training / window-search runs, newest first."""
    try:
        from src.db.database import list_training_runs
        kind = request.args.get("kind") or None
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
        return jsonify({"ok": True, "runs": list_training_runs(kind=kind, limit=limit)})
    except Exception as exc:
        logger.exception("Training runs error")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/training-runs/<int:run_id>")
def training_run_detail(run_id):
    """Full record (including metrics payload) for one run."""
    from src.db.database import get_training_run
    run = get_training_run(run_id)
    if run is None:
        return jsonify({"ok": False, "error": f"Run {run_id} not found"}), 404
    return jsonify({"ok": True, "run": run})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import threading
    import webbrowser

    port = 5000
    url = f"http://127.0.0.1:{port}"

    _get_artifacts()   # pre-load before serving

    threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    print(f"\n  CS2 Pick'em UI  →  {url}\n  Ctrl+C to stop.\n")
    app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    main()
