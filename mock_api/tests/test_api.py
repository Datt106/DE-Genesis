from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["records"] == 250


def test_pagination_is_deterministic() -> None:
    first = client.get("/api/v1/promotions?page=1&page_size=40").json()
    last = client.get("/api/v1/promotions?page=7&page_size=40").json()
    assert len(first["data"]) == 40
    assert len(last["data"]) == 10
    assert first["pagination"]["has_next"] is True
    assert last["pagination"]["has_next"] is False


def test_error_scenarios() -> None:
    assert client.get("/api/v1/promotions?scenario=rate_limit").status_code == 429
    assert client.get("/api/v1/promotions?scenario=server_error").status_code == 500


def test_incremental_window_is_half_open() -> None:
    included = client.get(
        "/api/v1/promotions",
        params={
            "updated_since": "2026-07-20T00:00:00Z",
            "updated_before": "2026-07-21T00:00:00Z",
        },
    ).json()
    excluded = client.get(
        "/api/v1/promotions",
        params={
            "updated_since": "2026-07-21T00:00:00Z",
            "updated_before": "2026-07-22T00:00:00Z",
        },
    ).json()
    assert included["pagination"]["total_records"] == 250
    assert excluded["pagination"]["total_records"] == 0
