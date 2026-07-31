from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2


VALID_DISCOUNT_TYPES = {"percentage", "fixed"}


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "de_roadmap"),
            user=os.getenv("POSTGRES_USER", "de_user"),
            password=os.environ["POSTGRES_PASSWORD"],
        )

    def connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )


def canonical_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_payload(payload).encode("utf-8")).hexdigest()


def parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def validate_promotion(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in ("promotion_id", "product_id", "discount_type", "discount_value", "starts_at", "ends_at", "version"):
        if payload.get(field) in (None, ""):
            errors.append(f"{field} không được để trống")
    discount_type = payload.get("discount_type")
    if discount_type and discount_type not in VALID_DISCOUNT_TYPES:
        errors.append("discount_type không hợp lệ")
    try:
        discount = float(payload.get("discount_value", 0))
        if discount < 0:
            errors.append("discount_value không được âm")
        if discount_type == "percentage" and discount > 100:
            errors.append("discount phần trăm phải nằm trong [0, 100]")
    except (TypeError, ValueError):
        errors.append("discount_value không phải số")
    try:
        starts_at = parse_datetime(payload.get("starts_at"))
        ends_at = parse_datetime(payload.get("ends_at"))
        if starts_at and ends_at and starts_at > ends_at:
            errors.append("starts_at phải trước hoặc bằng ends_at")
    except (TypeError, ValueError):
        errors.append("thời gian không đúng ISO-8601")
    try:
        if int(payload.get("version", 0)) < 1:
            errors.append("version phải lớn hơn hoặc bằng 1")
    except (TypeError, ValueError):
        errors.append("version không phải số nguyên")
    return errors


def calculate_discount(item_price: float, discount_type: str | None, discount_value: float) -> float:
    if discount_type == "percentage":
        return round(min(item_price, item_price * discount_value / 100), 2)
    if discount_type == "fixed":
        return round(min(item_price, discount_value), 2)
    return 0.0
