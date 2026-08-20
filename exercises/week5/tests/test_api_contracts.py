from pathlib import Path

import pytest

from exercises.week5.api_contracts import (
    RestAuth,
    build_soap_envelope,
    parse_soap_promotions,
    request_client_credentials_token,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "promotions_soap_response.xml"


class TokenResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"access_token": "token-123", "token_type": "Bearer"}


class TokenSession:
    def __init__(self):
        self.call = None

    def post(self, url, **kwargs):
        self.call = (url, kwargs)
        return TokenResponse()


def test_rest_auth_contracts_do_not_mix_credentials() -> None:
    assert RestAuth(mode="api_key", api_key="secret").headers() == {"X-API-Key": "secret"}
    assert RestAuth(mode="oauth2", access_token="abc").headers() == {
        "Authorization": "Bearer abc"
    }
    with pytest.raises(ValueError, match="Thiếu API key"):
        RestAuth(mode="api_key").headers()


def test_oauth2_client_credentials_contract() -> None:
    session = TokenSession()
    token = request_client_credentials_token(
        "https://identity.example/token",
        client_id="client",
        client_secret="secret",
        scope="promotion.read",
        session=session,
    )
    assert token == "token-123"
    _, kwargs = session.call
    assert kwargs["data"] == {
        "grant_type": "client_credentials",
        "scope": "promotion.read",
    }
    assert kwargs["auth"] == ("client", "secret")


def test_soap_fixture_is_normalized_to_rest_contract() -> None:
    records = parse_soap_promotions(FIXTURE.read_bytes())
    assert records[0]["promotion_id"] == "SOAP-001"
    assert records[0]["discount_type"] == "percentage"
    envelope = build_soap_envelope("ListPromotions", {"page": 1})
    assert b"Envelope" in envelope and b"ListPromotions" in envelope
