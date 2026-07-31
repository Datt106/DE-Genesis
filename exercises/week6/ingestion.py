from __future__ import annotations

import time
from typing import Any

import requests
from psycopg2.extras import Json

from exercises.week5.common import DatabaseConfig, parse_datetime, payload_hash, validate_promotion
from exercises.week6.repository import ensure_schema, update_counts


def fetch_incremental_promotions(
    api_url: str,
    *,
    window_start: str,
    window_end: str,
    scenario: str = "success",
    page_size: int = 100,
    timeout: float = 5,
    max_retries: int = 3,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    client = session or requests.Session()
    records: list[dict[str, Any]] = []
    page = 1
    while True:
        body: dict[str, Any] | None = None
        for attempt in range(1, max_retries + 1):
            try:
                response = client.get(
                    f"{api_url.rstrip('/')}/api/v1/promotions",
                    params={
                        "page": page,
                        "page_size": page_size,
                        "updated_since": window_start,
                        "updated_before": window_end,
                        "scenario": scenario,
                    },
                    timeout=timeout,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    response.raise_for_status()
                if 400 <= response.status_code < 500:
                    raise ValueError(f"API trả lỗi không retry: HTTP {response.status_code}")
                body = response.json()
                break
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
                if attempt >= max_retries:
                    raise
                time.sleep(min(2 ** (attempt - 1), 4))
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            raise ValueError("Response API không đúng contract")
        records.extend(body["data"])
        pagination = body.get("pagination", {})
        if not pagination.get("has_next", False):
            return records
        page += 1


def ingest_incremental_batch(
    config: dict[str, Any],
    *,
    api_url: str,
    page_size: int = 100,
) -> dict[str, int]:
    records = fetch_incremental_promotions(
        api_url,
        window_start=config["window_start"],
        window_end=config["window_end"],
        scenario=config["scenario"],
        page_size=page_size,
    )
    seen: set[tuple[str | None, str]] = set()
    accepted = rejected = 0
    prepared: list[tuple[Any, ...]] = []
    for record_index, payload in enumerate(records):
        errors = validate_promotion(payload)
        digest = payload_hash(payload)
        duplicate_key = (payload.get("promotion_id"), digest)
        if duplicate_key in seen:
            errors.append("duplicate payload trong cùng batch")
        seen.add(duplicate_key)
        is_valid = not errors
        accepted += int(is_valid)
        rejected += int(not is_valid)
        prepared.append(
            (
                config["batch_id"],
                record_index,
                payload.get("promotion_id"),
                payload.get("product_id"),
                Json(payload),
                parse_datetime(payload.get("updated_at")),
                digest,
                is_valid,
                "; ".join(errors) or None,
            )
        )

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            for row in prepared:
                cursor.execute(
                    """
                    INSERT INTO week6_raw.promotions(
                        batch_id, record_index, promotion_id, product_id, payload,
                        source_updated_at, payload_hash, is_valid, validation_error
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (batch_id,record_index) DO UPDATE
                    SET promotion_id=EXCLUDED.promotion_id,
                        product_id=EXCLUDED.product_id,
                        payload=EXCLUDED.payload,
                        source_updated_at=EXCLUDED.source_updated_at,
                        payload_hash=EXCLUDED.payload_hash,
                        is_valid=EXCLUDED.is_valid,
                        validation_error=EXCLUDED.validation_error,
                        ingested_at=NOW()
                    """,
                    row,
                )
            cursor.execute(
                "DELETE FROM week6_raw.promotions WHERE batch_id=%s AND record_index >= %s",
                (config["batch_id"], len(prepared)),
            )

    result = {
        "raw_count": len(records),
        "accepted_count": accepted,
        "rejected_count": rejected,
    }
    update_counts(config["run_id"], status="raw_ready", **result)
    return result
