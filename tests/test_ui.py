import pytest


@pytest.fixture
def client(tmp_db):
    from src.ui import app
    app.config["TESTING"] = True
    return app.test_client()


def test_training_runs_endpoints(client, tmp_db):
    run_id = tmp_db.record_training_run("train", "logistic_regression", 0.62, {"n_folds": 5}, {"ok": 1})
    r = client.get("/api/training-runs").get_json()
    assert r["ok"] and r["runs"][0]["run_id"] == run_id
    assert client.get("/api/training-runs?kind=window_search").get_json()["runs"] == []
    detail = client.get(f"/api/training-runs/{run_id}").get_json()
    assert detail["run"]["result"] == {"ok": 1}
    assert client.get("/api/training-runs/999").status_code == 404


def test_fetch_data_rejects_unknown_operation(client):
    r = client.post("/api/fetch-data", json={"operation": "nope"})
    assert r.status_code == 400
