from exercises.week6.ingestion import fetch_incremental_promotions


class FakeResponse:
    def __init__(self, body, status_code=200):
        self._body = body
        self.status_code = status_code

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append((url, params, timeout))
        page = params["page"]
        return FakeResponse(
            {
                "data": [{"promotion_id": f"PROMO-{page}"}],
                "pagination": {"has_next": page == 1},
            }
        )


def test_incremental_fetch_passes_half_open_window_and_paginates() -> None:
    session = FakeSession()
    records = fetch_incremental_promotions(
        "http://mock-api:8000",
        window_start="2026-07-20T00:00:00+00:00",
        window_end="2026-07-21T00:00:00+00:00",
        page_size=25,
        session=session,
    )
    assert [row["promotion_id"] for row in records] == ["PROMO-1", "PROMO-2"]
    assert len(session.calls) == 2
    assert session.calls[0][1]["updated_since"] == "2026-07-20T00:00:00+00:00"
    assert session.calls[0][1]["updated_before"] == "2026-07-21T00:00:00+00:00"
    assert session.calls[0][1]["page_size"] == 25
