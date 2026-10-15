import pytest
from fastapi.testclient import TestClient

from src.api import main
from src.data.rows import planted_purchase


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("WEX_DB", str(tmp_path / "api.duckdb"))
    main.reset_db()
    with TestClient(main.app) as test_client:
        yield test_client
    main.reset_db()


def test_t11_health_does_not_open_the_database(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("health touched the database")

    monkeypatch.setattr(main, "connect", boom)
    main.reset_db()
    with TestClient(main.app) as test_client:
        response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    main.reset_db()


def test_ready_and_run_and_ask(client):
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert ready.json()["catalog_version"] == "2026.10.15"

    created = client.post(
        "/v1/pipelines/fleet_transactions_daily/runs",
        json={"batch_id": "api-batch", "rows": planted_purchase()},
    )
    assert created.status_code == 202
    body = created.json()
    assert body["status"] == "committed"
    assert body["inserted"] == 4

    proof = client.get(
        "/v1/runs/%s/proof" % body["run_id"],
        params={"check": "purchase", "purchase_id": "GRP-77"},
        headers={"X-Persona": "data_engineer"},
    )
    assert proof.status_code == 200
    assert proof.json()["broken"] == 0

    asked = client.post(
        "/v1/ask",
        json={"question": "What is the net settled amount for GRP-77?", "engine": "rag"},
        headers={"X-Persona": "business_analyst"},
    )
    assert asked.status_code == 200
    assert asked.json()["rows"][0]["value"] == pytest.approx(45.99)
    assert "SELECT" in asked.json()["sql"]
