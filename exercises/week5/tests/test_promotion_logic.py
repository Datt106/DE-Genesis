from datetime import datetime, timezone

from exercises.week5.common import calculate_discount, payload_hash, validate_promotion


VALID = {
    "promotion_id": "PROMO-1",
    "product_id": "PRODUCT-1",
    "discount_type": "percentage",
    "discount_value": 10,
    "starts_at": "2017-01-01T00:00:00Z",
    "ends_at": "2017-12-31T23:59:59Z",
    "version": 1,
}


def test_valid_promotion() -> None:
    assert validate_promotion(VALID) == []


def test_invalid_interval_and_discount() -> None:
    invalid = {
        **VALID,
        "discount_value": 120,
        "starts_at": "2018-01-01T00:00:00Z",
        "ends_at": "2017-01-01T00:00:00Z",
    }
    errors = validate_promotion(invalid)
    assert "discount phần trăm phải nằm trong [0, 100]" in errors
    assert "starts_at phải trước hoặc bằng ends_at" in errors


def test_discount_never_exceeds_item_price() -> None:
    assert calculate_discount(100, "percentage", 10) == 10
    assert calculate_discount(7, "fixed", 20) == 7
    assert calculate_discount(100, None, 50) == 0


def test_payload_hash_is_order_independent() -> None:
    assert payload_hash({"a": 1, "b": 2}) == payload_hash({"b": 2, "a": 1})
