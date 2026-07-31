from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from psycopg2.extras import Json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig, parse_datetime, payload_hash, validate_promotion


SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "create_week5_schemas.sql"


def ensure_schema(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def fetch_all_promotions(
    api_url: str,
    *,
    page_size: int = 100,
    scenario: str = "success",
    timeout: float = 5,
    max_retries: int = 3,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    client = session or requests.Session()
    records: list[dict[str, Any]] = []
    page = 1
    while True:
        for attempt in range(1, max_retries + 1):
            try:
                response = client.get(
                    f"{api_url.rstrip('/')}/api/v1/promotions",
                    params={"page": page, "page_size": page_size, "scenario": scenario},
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


def ingest_airflow_batch(
    *,
    run_id: str,
    batch_id: str,
    scenario: str = "success",
    api_url: str | None = None,
    page_size: int | None = None,
) -> dict[str, int]:
    database = DatabaseConfig.from_env()
    records = fetch_all_promotions(
        api_url or os.getenv("MOCK_API_URL", "http://localhost:8000"),
        page_size=page_size or int(os.getenv("WEEK5_API_PAGE_SIZE", "100")),
        scenario=scenario,
    )
    raw_count = accepted = rejected = 0
    with database.connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO week5_control.pipeline_runs(
                    run_id, pipeline_name, source_mode, batch_id, status
                ) VALUES (%s, %s, 'airflow', %s, 'running')
                ON CONFLICT (source_mode, batch_id) DO UPDATE
                SET run_id = EXCLUDED.run_id, status = 'running',
                    started_at = NOW(), finished_at = NULL, error_message = NULL
                """,
                (run_id, "de_genesis_week5_airflow_ingestion", batch_id),
            )
            for payload in records:
                errors = validate_promotion(payload)
                is_valid = not errors
                raw_count += 1
                accepted += int(is_valid)
                rejected += int(not is_valid)
                cursor.execute(
                    """
                    INSERT INTO week5_raw.promotions_airflow(
                        batch_id, source_system, promotion_id, product_id, payload,
                        source_updated_at, payload_hash, is_valid, validation_error
                    ) VALUES (%s, 'airflow', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_system, promotion_id, payload_hash) DO NOTHING
                    """,
                    (
                        batch_id,
                        payload.get("promotion_id"),
                        payload.get("product_id"),
                        Json(payload),
                        parse_datetime(payload.get("updated_at")),
                        payload_hash(payload),
                        is_valid,
                        "; ".join(errors) or None,
                    ),
                )
            cursor.execute(
                """
                UPDATE week5_control.pipeline_runs
                SET status = 'raw_ready', raw_count = %s,
                    accepted_count = %s, rejected_count = %s
                WHERE run_id = %s
                """,
                (raw_count, accepted, rejected, run_id),
            )
    return {"raw_count": raw_count, "accepted_count": accepted, "rejected_count": rejected}


def mark_failure(run_id: str, error_message: str) -> None:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        ensure_schema(connection)
        cursor.execute(
            """
            UPDATE week5_control.pipeline_runs
            SET status = 'failed', finished_at = NOW(), error_message = %s
            WHERE run_id = %s
            """,
            (error_message[:4000], run_id),
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Nạp promotion từ mock API bằng Airflow-centric flow")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--scenario", default="success")
    arguments = parser.parse_args()
    print(json.dumps(ingest_airflow_batch(**vars(arguments)), ensure_ascii=False))
