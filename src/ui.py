"""Web UI for the CS2 Pick'em Simulator."""

import logging
import os
import threading

from flask import Flask, jsonify, render_template, request

from src.pickem import run_pickem
from src.predict import load_artifacts
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


class _RetrainLogHandler(logging.Handler):
    """Appends formatted log records into the retrain state log list."""

    def emit(self, record):
        msg = self.format(record)
        with _retrain_lock:
            _retrain_state["logs"].append(msg)


def _run_retrain(cutoff_date):
    """Background thread: rebuild features then train."""
    handler = _RetrainLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    root = logging.getLogger("cs2predictor")
    root.addHandler(handler)

    try:
        from src.features.build_features import build_feature_matrix
        from src.model.train import train_pipeline

        build_feature_matrix(cutoff_date=cutoff_date)
        metadata = train_pipeline(cutoff_date=cutoff_date)

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
        cutoff_date = data.get("cutoff_date") or None   # YYYY-MM-DD or None

        _retrain_state.update({
            "status": "running",
            "logs": [],
            "metrics": None,
            "error": None,
        })

    thread = threading.Thread(target=_run_retrain, args=(cutoff_date,), daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.route("/api/retrain/status")
def retrain_status():
    """Return current retrain job state (for polling)."""
    with _retrain_lock:
        return jsonify(dict(_retrain_state))


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
