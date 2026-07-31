from __future__ import annotations

import pytest
import requests

from exercises.week5.ingestion import fetch_all_promotions


class FakeResponse:
    def __init__(self, status_code: int, body: dict):
        self.status_code = status_code
        self._body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        return next(self.responses)


def page(records, current, has_next):
    return {"data": records, "pagination": {"page": current, "has_next": has_next}}


def test_fetches_all_pages() -> None:
    session = FakeSession(
        [
            FakeResponse(200, page([{"promotion_id": "1"}], 1, True)),
            FakeResponse(200, page([{"promotion_id": "2"}], 2, False)),
        ]
    )
    records = fetch_all_promotions("http://mock", session=session)
    assert [record["promotion_id"] for record in records] == ["1", "2"]
    assert session.calls == 2


def test_retries_transient_http(monkeypatch) -> None:
    monkeypatch.setattr("exercises.week5.ingestion.time.sleep", lambda _: None)
    session = FakeSession(
        [
            FakeResponse(500, {}),
            FakeResponse(200, page([], 1, False)),
        ]
    )
    assert fetch_all_promotions("http://mock", session=session) == []
    assert session.calls == 2


def test_rejects_contract_violation() -> None:
    session = FakeSession([FakeResponse(200, {"data": "not-a-list"})])
    with pytest.raises(ValueError, match="contract"):
        fetch_all_promotions("http://mock", session=session)
