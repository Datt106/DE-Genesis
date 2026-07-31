from __future__ import annotations

from pathlib import Path
from typing import Any

from exercises.week5.common import DatabaseConfig


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
            SET previous_window_end=week6_control.ingestion_watermarks.current_window_end,
                current_window_end=EXCLUDED.current_window_end,
                run_id=EXCLUDED.run_id,
                updated_at=NOW()
            """,
            (config["window_end"], config["run_id"]),
        )
