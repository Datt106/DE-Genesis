from __future__ import annotations

from pathlib import Path
from typing import Any

from exercises.week5.common import DatabaseConfig
from exercises.week6.config import parse_utc


SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "create_week6_schemas.sql"


def ensure_schema(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


def initialize_run(config: dict[str, Any], pipeline_name: str) -> None:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_window_end
                FROM week6_control.ingestion_watermarks
                WHERE pipeline_name=%s
                FOR UPDATE
                """,
                (pipeline_name,),
            )
            watermark_row = cursor.fetchone()
            if config.get("run_mode") == "scheduled" and watermark_row is not None:
                expected_start = parse_utc(watermark_row[0], "current_watermark")
                actual_start = parse_utc(config["window_start"], "window_start")
                if actual_start != expected_start:
                    raise RuntimeError(
                        "Cửa sổ scheduled không nối tiếp watermark; "
                        "hãy resolve lại cấu hình để lấp gap"
                    )
            cursor.execute(
                """
                INSERT INTO week6_control.pipeline_runs(
                    run_id, pipeline_name, batch_id, window_start, window_end,
                    scenario, invalid_rate_threshold, status, attempt_number
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'running',1)
                ON CONFLICT (run_id) DO UPDATE
                SET pipeline_name=EXCLUDED.pipeline_name,
                    batch_id=EXCLUDED.batch_id,
                    window_start=EXCLUDED.window_start,
                    window_end=EXCLUDED.window_end,
                    scenario=EXCLUDED.scenario,
                    invalid_rate_threshold=EXCLUDED.invalid_rate_threshold,
                    status='running',
                    attempt_number=week6_control.pipeline_runs.attempt_number + 1,
                    started_at=NOW(),
                    finished_at=NULL,
                    error_message=NULL
                """,
                (
                    config["run_id"],
                    pipeline_name,
                    config["batch_id"],
                    config["window_start"],
                    config["window_end"],
                    config["scenario"],
                    config["invalid_rate_threshold"],
                ),
            )


def update_counts(run_id: str, *, status: str, **counts: int) -> None:
    allowed = {"raw_count", "accepted_count", "rejected_count", "curated_count"}
    unknown = set(counts) - allowed
    if unknown:
        raise ValueError(f"Trường count không được hỗ trợ: {sorted(unknown)}")
    assignments = ["status=%s", *[f"{name}=%s" for name in counts]]
    values: list[Any] = [status, *counts.values(), run_id]
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"UPDATE week6_control.pipeline_runs SET {', '.join(assignments)} WHERE run_id=%s",
            values,
        )


def mark_failure(run_id: str, error_message: str) -> None:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE week6_control.pipeline_runs
                SET status='failed', finished_at=NOW(), error_message=%s
                WHERE run_id=%s AND status <> 'success'
                """,
                (error_message[:4000], run_id),
            )


def finalize_success(config: dict[str, Any]) -> None:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM week6_control.quality_results
            WHERE run_id=%s AND check_status='failed'
            """,
            (config["run_id"],),
        )
        failure_count = cursor.fetchone()[0]
        if failure_count:
            raise RuntimeError(f"Không thể hoàn tất run vì còn {failure_count} quality check thất bại")
        cursor.execute(
            """
            UPDATE week6_control.pipeline_runs
            SET status='success', finished_at=NOW()
            WHERE run_id=%s
            """,
            (config["run_id"],),
        )
        cursor.execute(
            """
            INSERT INTO week6_control.ingestion_watermarks(
                pipeline_name, previous_window_end, current_window_end, run_id
            ) VALUES ('de_genesis_week6_production_pipeline',NULL,%s,%s)
            ON CONFLICT (pipeline_name) DO UPDATE
            SET previous_window_end=CASE
                    WHEN EXCLUDED.current_window_end
                         > week6_control.ingestion_watermarks.current_window_end
                    THEN week6_control.ingestion_watermarks.current_window_end
                    ELSE week6_control.ingestion_watermarks.previous_window_end
                END,
                current_window_end=GREATEST(
                    week6_control.ingestion_watermarks.current_window_end,
                    EXCLUDED.current_window_end
                ),
                run_id=CASE
                    WHEN EXCLUDED.current_window_end
                         > week6_control.ingestion_watermarks.current_window_end
                    THEN EXCLUDED.run_id
                    ELSE week6_control.ingestion_watermarks.run_id
                END,
                updated_at=CASE
                    WHEN EXCLUDED.current_window_end
                         > week6_control.ingestion_watermarks.current_window_end
                    THEN NOW()
                    ELSE week6_control.ingestion_watermarks.updated_at
                END
            WHERE %s::timestamptz
                  <= week6_control.ingestion_watermarks.current_window_end
            """,
            (config["window_end"], config["run_id"], config["window_start"]),
        )


def get_watermark(pipeline_name: str) -> Any | None:
    """Đọc watermark hiện hành; trả ``None`` khi schema/run đầu chưa có."""

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_window_end
                FROM week6_control.ingestion_watermarks
                WHERE pipeline_name=%s
                """,
                (pipeline_name,),
            )
            row = cursor.fetchone()
            return row[0] if row else None


def publish_curated_snapshot(config: dict[str, Any]) -> dict[str, int | str]:
    """Publish snapshot đã đạt DQ bằng một transaction có advisory lock."""

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT accepted_count, curated_count
                FROM week6_control.pipeline_runs
                WHERE run_id=%s
                FOR UPDATE
                """,
                (config["run_id"],),
            )
            audit = cursor.fetchone()
            if audit is None:
                raise RuntimeError("Không tìm thấy audit run trước khi publish")
            accepted_count, audited_curated = audit
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM week6_control.quality_results
                WHERE run_id=%s AND check_status='failed'
                """,
                (config["run_id"],),
            )
            if cursor.fetchone()[0]:
                raise RuntimeError("Không publish vì quality gate còn lỗi")

            if accepted_count == 0:
                return {"mode": "no-op", "published_count": audited_curated}

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM week6_curated.sales_promotion_staging
                WHERE run_id=%s
                """,
                (config["run_id"],),
            )
            staging_count = cursor.fetchone()[0]
            if staging_count != audited_curated:
                raise RuntimeError(
                    "Số dòng staging thay đổi sau quality gate; hủy publish"
                )

            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("week6_curated.sales_promotion",),
            )
            cursor.execute("DELETE FROM week6_curated.sales_promotion")
            cursor.execute(
                """
                INSERT INTO week6_curated.sales_promotion(
                    run_id, batch_id, order_id, item_number, product_id,
                    purchase_date, promotion_id, promotion_version, discount_type,
                    discount_value, item_price, freight_value, gross_amount,
                    discount_amount, net_amount_after_discount, processed_at
                )
                SELECT run_id, batch_id, order_id, item_number, product_id,
                       purchase_date, promotion_id, promotion_version, discount_type,
                       discount_value, item_price, freight_value, gross_amount,
                       discount_amount, net_amount_after_discount, processed_at
                FROM week6_curated.sales_promotion_staging
                WHERE run_id=%s
                """,
                (config["run_id"],),
            )
            if cursor.rowcount != staging_count:
                raise RuntimeError("Publish không ghi đủ số dòng staging")
            cursor.execute(
                "DELETE FROM week6_curated.sales_promotion_staging WHERE run_id=%s",
                (config["run_id"],),
            )
            return {"mode": "atomic-replace", "published_count": staging_count}
