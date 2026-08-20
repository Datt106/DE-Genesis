"""Persistence và atomic-publish contract cho pipeline service log."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from exercises.week5.common import DatabaseConfig
from exercises.week6.log_contracts import (
    LogContractError,
    parse_utc,
    select_stream_lineage_id,
    stream_batch_sequence_action,
)
from exercises.week6.repository import ensure_schema


LOG_REPORT_PIPELINE = "de_genesis_week6_log_report"
EXPECTED_LOG_QUALITY_CHECKS = 8


class CheckpointGenerationError(RuntimeError):
    """Checkpoint generation bị tái dùng sau khi epoch đã quay lùi."""


def prepare_stream_batch(
    stream_generation_id: str,
    stream_batch_id: int,
    query_name: str,
    checkpoint_path: str,
) -> bool:
    """Reserve epoch; trả False khi chỉ replay epoch cuối đã publish.

    Batch ID nhỏ hơn high-water mark chứng minh checkpoint đã reset. Job dừng
    trước khi ghi HDFS/JDBC và yêu cầu operator đổi generation ID.
    """

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"week6-log-generation-{query_name}",),
            )
            cursor.execute(
                """
                INSERT INTO week6_control.log_stream_generations(
                    stream_generation_id, query_name, checkpoint_path,
                    lineage_id, is_active
                ) VALUES (%s,%s,%s,%s,FALSE)
                ON CONFLICT (stream_generation_id) DO NOTHING
                """,
                (
                    stream_generation_id,
                    query_name,
                    checkpoint_path,
                    stream_generation_id,
                ),
            )
            cursor.execute(
                """
                SELECT query_name, checkpoint_path, last_successful_batch_id,
                       is_active
                FROM week6_control.log_stream_generations
                WHERE stream_generation_id=%s
                FOR UPDATE
                """,
                (stream_generation_id,),
            )
            (
                stored_query,
                stored_checkpoint,
                last_successful_batch_id,
                is_active,
            ) = cursor.fetchone()
            if stored_query != query_name or stored_checkpoint != checkpoint_path:
                raise CheckpointGenerationError(
                    "stream_generation_id đã gắn với query/checkpoint khác; "
                    "hãy cấp generation ID mới"
                )
            if not is_active and last_successful_batch_id >= 0:
                raise CheckpointGenerationError(
                    "Generation đã bị generation mới thay thế; không được resume "
                    "checkpoint cũ"
                )
            existing_status = None
            if stream_batch_id == last_successful_batch_id:
                cursor.execute(
                    """
                    SELECT status
                    FROM week6_control.log_stream_batches
                    WHERE stream_generation_id=%s AND stream_batch_id=%s
                    """,
                    (stream_generation_id, stream_batch_id),
                )
                existing = cursor.fetchone()
                existing_status = existing[0] if existing else None
            try:
                sequence_action = stream_batch_sequence_action(
                    stream_batch_id,
                    last_successful_batch_id,
                    existing_status,
                )
            except LogContractError as exc:
                raise CheckpointGenerationError(
                    f"{exc}; đổi WEEK6_LOG_GENERATION_ID trước khi chạy lại"
                ) from exc
            if sequence_action == "replay":
                return False
            cursor.execute(
                "DELETE FROM week6_log.requests_per_minute_stream_staging "
                "WHERE stream_generation_id=%s AND stream_batch_id=%s",
                (stream_generation_id, stream_batch_id),
            )
            cursor.execute(
                "DELETE FROM week6_log.status_distribution_stream_staging "
                "WHERE stream_generation_id=%s AND stream_batch_id=%s",
                (stream_generation_id, stream_batch_id),
            )
            cursor.execute(
                """
                INSERT INTO week6_control.log_stream_batches(
                    stream_generation_id, stream_batch_id, query_name, status,
                    started_at, finished_at, max_event_time, raw_count,
                    valid_count, invalid_count, ingestion_lag_seconds, error_message
                ) VALUES (%s,%s,%s,'running',NOW(),NULL,NULL,0,0,0,NULL,NULL)
                ON CONFLICT (stream_generation_id,stream_batch_id) DO UPDATE
                SET query_name=EXCLUDED.query_name,
                    status='running',
                    started_at=NOW(),
                    finished_at=NULL,
                    error_message=NULL
                """,
                (stream_generation_id, stream_batch_id, query_name),
            )
    return True


def publish_stream_batch(
    *,
    stream_generation_id: str,
    stream_batch_id: int,
    query_name: str,
    raw_count: int,
    valid_count: int,
    invalid_count: int,
    max_event_time: datetime | None,
    ingestion_lag_seconds: float | None,
) -> None:
    """Đổi contribution của một epoch và telemetry trong cùng transaction."""

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"week6-log-generation-{query_name}",),
            )
            cursor.execute(
                """
                SELECT is_active, last_successful_batch_id
                FROM week6_control.log_stream_generations
                WHERE stream_generation_id=%s AND query_name=%s
                FOR UPDATE
                """,
                (stream_generation_id, query_name),
            )
            generation = cursor.fetchone()
            if generation is None:
                raise CheckpointGenerationError("Generation chưa được reserve")
            is_active, last_successful_batch_id = generation
            if not is_active and last_successful_batch_id >= 0:
                raise CheckpointGenerationError(
                    "Generation đã bị thay thế trước khi batch kịp publish"
                )
            if not is_active:
                cursor.execute(
                    """
                    SELECT last_successful_batch_id, lineage_id
                    FROM week6_control.log_stream_generations
                    WHERE query_name=%s AND is_active
                    FOR UPDATE
                    """,
                    (query_name,),
                )
                active_generation = cursor.fetchone()
                lineage_id = select_stream_lineage_id(
                    stream_generation_id=stream_generation_id,
                    stream_batch_id=stream_batch_id,
                    active_last_successful_batch_id=(
                        active_generation[0]
                        if active_generation is not None
                        else None
                    ),
                    active_lineage_id=(
                        active_generation[1]
                        if active_generation is not None
                        else None
                    ),
                )
                cursor.execute(
                    """
                    UPDATE week6_control.log_stream_generations
                    SET lineage_id=%s
                    WHERE stream_generation_id=%s
                    """,
                    (lineage_id, stream_generation_id),
                )
            for table in (
                "week6_log.requests_per_minute_stream",
                "week6_log.status_distribution_stream",
            ):
                cursor.execute(
                    f"DELETE FROM {table} "
                    "WHERE stream_generation_id=%s AND stream_batch_id=%s",
                    (stream_generation_id, stream_batch_id),
                )
            cursor.execute(
                """
                INSERT INTO week6_log.requests_per_minute_stream(
                    stream_generation_id, stream_batch_id, minute_start, service,
                    request_count, latency_sum_ms, max_latency_ms
                )
                SELECT stream_generation_id, stream_batch_id, minute_start,
                       service, request_count, latency_sum_ms, max_latency_ms
                FROM week6_log.requests_per_minute_stream_staging
                WHERE stream_generation_id=%s AND stream_batch_id=%s
                """,
                (stream_generation_id, stream_batch_id),
            )
            cursor.execute(
                """
                INSERT INTO week6_log.status_distribution_stream(
                    stream_generation_id, stream_batch_id, minute_start,
                    service, status_code, request_count
                )
                SELECT stream_generation_id, stream_batch_id, minute_start,
                       service, status_code, request_count
                FROM week6_log.status_distribution_stream_staging
                WHERE stream_generation_id=%s AND stream_batch_id=%s
                """,
                (stream_generation_id, stream_batch_id),
            )
            cursor.execute(
                "DELETE FROM week6_log.requests_per_minute_stream_staging "
                "WHERE stream_generation_id=%s AND stream_batch_id=%s",
                (stream_generation_id, stream_batch_id),
            )
            cursor.execute(
                "DELETE FROM week6_log.status_distribution_stream_staging "
                "WHERE stream_generation_id=%s AND stream_batch_id=%s",
                (stream_generation_id, stream_batch_id),
            )
            cursor.execute(
                """
                UPDATE week6_control.log_stream_batches
                SET query_name=%s,
                    status='success',
                    finished_at=NOW(),
                    max_event_time=%s,
                    raw_count=%s,
                    valid_count=%s,
                    invalid_count=%s,
                    ingestion_lag_seconds=%s,
                    error_message=NULL
                WHERE stream_generation_id=%s AND stream_batch_id=%s
                """,
                (
                    query_name,
                    max_event_time,
                    raw_count,
                    valid_count,
                    invalid_count,
                    ingestion_lag_seconds,
                    stream_generation_id,
                    stream_batch_id,
                ),
            )
            cursor.execute(
                """
                UPDATE week6_control.log_stream_generations
                SET is_active=FALSE, updated_at=NOW()
                WHERE query_name=%s AND stream_generation_id<>%s AND is_active
                """,
                (query_name, stream_generation_id),
            )
            cursor.execute(
                """
                UPDATE week6_control.log_stream_generations
                SET last_successful_batch_id=GREATEST(last_successful_batch_id,%s),
                    is_active=TRUE, updated_at=NOW()
                WHERE stream_generation_id=%s
                """,
                (stream_batch_id, stream_generation_id),
            )


def fail_stream_batch(
    stream_generation_id: str,
    stream_batch_id: int,
    query_name: str,
    error: str,
) -> None:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO week6_control.log_stream_batches(
                    stream_generation_id, stream_batch_id, query_name, status,
                    finished_at, error_message
                ) VALUES (%s,%s,%s,'failed',NOW(),%s)
                ON CONFLICT (stream_generation_id,stream_batch_id) DO UPDATE
                SET query_name=EXCLUDED.query_name,
                    status='failed',
                    finished_at=NOW(),
                    error_message=EXCLUDED.error_message
                WHERE week6_control.log_stream_batches.status <> 'success'
                """,
                (stream_generation_id, stream_batch_id, query_name, error[:4000]),
            )


def initialize_log_report(config: dict[str, Any]) -> None:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_window_end
                FROM week6_control.log_report_watermark
                WHERE pipeline_name=%s
                FOR UPDATE
                """,
                (LOG_REPORT_PIPELINE,),
            )
            watermark = cursor.fetchone()
            if config["mode"] == "scheduled" and watermark is not None:
                if parse_utc(config["window_start"], "window_start") != parse_utc(
                    watermark[0], "current_watermark"
                ):
                    raise RuntimeError("Cửa sổ log report không nối tiếp watermark")
            cursor.execute(
                """
                INSERT INTO week6_control.log_report_runs(
                    run_id, window_start, window_end, mode, settlement_seconds,
                    data_available_through, hdfs_staging_path,
                    hdfs_published_path, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'running')
                ON CONFLICT (run_id) DO UPDATE
                SET window_start=EXCLUDED.window_start,
                    window_end=EXCLUDED.window_end,
                    mode=EXCLUDED.mode,
                    settlement_seconds=EXCLUDED.settlement_seconds,
                    data_available_through=EXCLUDED.data_available_through,
                    hdfs_staging_path=EXCLUDED.hdfs_staging_path,
                    hdfs_published_path=EXCLUDED.hdfs_published_path,
                    status='running',
                    started_at=NOW(),
                    finished_at=NULL,
                    source_count=0,
                    minute_report_count=0,
                    status_report_count=0,
                    error_message=NULL
                """,
                (
                    config["run_id"],
                    config["window_start"],
                    config["window_end"],
                    config["mode"],
                    config["settlement_seconds"],
                    config["data_available_through"],
                    config["hdfs_staging_path"],
                    config["hdfs_published_path"],
                ),
            )
            cursor.execute(
                "DELETE FROM week6_log.requests_per_minute_staging WHERE run_id=%s",
                (config["run_id"],),
            )
            cursor.execute(
                "DELETE FROM week6_log.status_distribution_staging WHERE run_id=%s",
                (config["run_id"],),
            )
            cursor.execute(
                "DELETE FROM week6_control.log_quality_results WHERE run_id=%s",
                (config["run_id"],),
            )


def get_log_report_watermark() -> Any | None:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_window_end
                FROM week6_control.log_report_watermark
                WHERE pipeline_name=%s
                """,
                (LOG_REPORT_PIPELINE,),
            )
            row = cursor.fetchone()
            return row[0] if row else None


def update_log_report_counts(
    run_id: str,
    *,
    source_count: int,
    minute_report_count: int,
    status_report_count: int,
) -> None:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE week6_control.log_report_runs
            SET status='transforming',
                source_count=%s,
                minute_report_count=%s,
                status_report_count=%s
            WHERE run_id=%s
            """,
            (source_count, minute_report_count, status_report_count, run_id),
        )


def prepare_log_report_staging(run_id: str) -> None:
    """Làm cho retry của Spark report idempotent mà không xóa audit run."""

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM week6_log.requests_per_minute_staging WHERE run_id=%s",
                (run_id,),
            )
            cursor.execute(
                "DELETE FROM week6_log.status_distribution_staging WHERE run_id=%s",
                (run_id,),
            )
            cursor.execute(
                """
                UPDATE week6_control.log_report_runs
                SET status='transforming', error_message=NULL
                WHERE run_id=%s
                """,
                (run_id,),
            )


def mark_log_report_failure(run_id: str, error: str) -> None:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE week6_control.log_report_runs
                SET status='failed', finished_at=NOW(), error_message=%s
                WHERE run_id=%s AND status <> 'success'
                """,
                (error[:4000], run_id),
            )


def assert_log_report_quality_ready(run_id: str) -> None:
    """Chặn publish nếu DAG bỏ qua hoặc chỉ chạy một phần quality gate."""

    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        _assert_log_report_quality_ready(cursor, run_id)


def _assert_log_report_quality_ready(cursor: Any, run_id: str) -> None:
    cursor.execute(
        """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE check_status='passed'),
               COUNT(*) FILTER (WHERE check_status='failed')
        FROM week6_control.log_quality_results
        WHERE run_id=%s
        """,
        (run_id,),
    )
    total, passed, failed = cursor.fetchone()
    if failed or total != EXPECTED_LOG_QUALITY_CHECKS or passed != total:
        raise RuntimeError(
            "Quality gate log chưa hoàn tất đủ "
            f"{EXPECTED_LOG_QUALITY_CHECKS} kiểm tra thành công "
            f"(total={total}, passed={passed}, failed={failed})"
        )


def latest_stream_health(
    max_age_seconds: int = 90,
    max_running_seconds: int = 120,
) -> dict[str, Any]:
    """Contract health dùng cho Airflow và exporter."""

    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT stream_generation_id, stream_batch_id, started_at
                FROM week6_control.log_stream_batches
                WHERE status='running'
                  AND started_at < NOW() - (%s * INTERVAL '1 second')
                ORDER BY started_at
                LIMIT 1
                """,
                (max_running_seconds,),
            )
            stuck = cursor.fetchone()
            if stuck is not None:
                raise RuntimeError(
                    f"Log streaming batch {stuck[0]}/{stuck[1]} chạy quá "
                    f"{max_running_seconds} giây"
                )
            cursor.execute(
                """
                SELECT stream_generation_id, stream_batch_id, status, finished_at,
                       ingestion_lag_seconds, invalid_count
                FROM week6_control.log_stream_batches
                WHERE finished_at IS NOT NULL
                ORDER BY finished_at DESC, stream_batch_id DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Chưa có telemetry từ log streaming")
    generation_id, batch_id, status, finished_at, lag, invalid_count = row
    age = (
        datetime.now(timezone.utc) - finished_at
    ).total_seconds() if finished_at else float("inf")
    if status != "success":
        raise RuntimeError(f"Log streaming batch {batch_id} có trạng thái {status}")
    if age > max_age_seconds:
        raise RuntimeError(
            f"Log streaming không có heartbeat mới trong {age:.1f} giây"
        )
    if lag is not None and float(lag) > 60:
        raise RuntimeError(f"Độ trễ event-to-HDFS là {float(lag):.3f}s, vượt SLA 60s")
    return {
        "stream_batch_id": batch_id,
        "stream_generation_id": generation_id,
        "age_seconds": age,
        "ingestion_lag_seconds": float(lag) if lag is not None else None,
        "invalid_count": invalid_count,
    }


def publish_log_report(config: dict[str, Any]) -> dict[str, int]:
    """Publish report cửa sổ đóng; backfill cũ không làm lùi watermark."""

    assert_log_report_quality_ready(config["run_id"])
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT minute_report_count, status_report_count, status
                FROM week6_control.log_report_runs
                WHERE run_id=%s
                FOR UPDATE
                """,
                (config["run_id"],),
            )
            audit = cursor.fetchone()
            if audit is None:
                raise RuntimeError("Không tìm thấy log report run")
            minute_count, status_count, run_status = audit
            if run_status == "success":
                return {
                    "minute_report_count": minute_count,
                    "status_report_count": status_count,
                }
            _assert_log_report_quality_ready(cursor, config["run_id"])
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("week6-log-report",),
            )
            cursor.execute(
                """
                DELETE FROM week6_log.requests_per_minute
                WHERE minute_start >= %s AND minute_start < %s
                """,
                (config["window_start"], config["window_end"]),
            )
            cursor.execute(
                """
                DELETE FROM week6_log.status_distribution
                WHERE minute_start >= %s AND minute_start < %s
                """,
                (config["window_start"], config["window_end"]),
            )
            cursor.execute(
                """
                INSERT INTO week6_log.requests_per_minute(
                    minute_start, service, request_count, avg_latency_ms,
                    max_latency_ms, run_id
                )
                SELECT minute_start, service, request_count, avg_latency_ms,
                       max_latency_ms, run_id
                FROM week6_log.requests_per_minute_staging
                WHERE run_id=%s
                """,
                (config["run_id"],),
            )
            if cursor.rowcount != minute_count:
                raise RuntimeError("Publish requests/phút không khớp audit")
            cursor.execute(
                """
                INSERT INTO week6_log.status_distribution(
                    minute_start, service, status_code, request_count,
                    percentage, run_id
                )
                SELECT minute_start, service, status_code, request_count,
                       percentage, run_id
                FROM week6_log.status_distribution_staging
                WHERE run_id=%s
                """,
                (config["run_id"],),
            )
            if cursor.rowcount != status_count:
                raise RuntimeError("Publish phân bố status không khớp audit")
            cursor.execute(
                "DELETE FROM week6_log.requests_per_minute_staging WHERE run_id=%s",
                (config["run_id"],),
            )
            cursor.execute(
                "DELETE FROM week6_log.status_distribution_staging WHERE run_id=%s",
                (config["run_id"],),
            )
            cursor.execute(
                """
                UPDATE week6_control.log_report_runs
                SET status='success', finished_at=NOW(), error_message=NULL,
                    hdfs_staging_path=%s,
                    hdfs_published_path=%s
                WHERE run_id=%s
                """,
                (
                    config["hdfs_staging_path"],
                    config["hdfs_published_path"],
                    config["run_id"],
                ),
            )
            cursor.execute(
                """
                INSERT INTO week6_control.log_report_watermark(
                    pipeline_name, current_window_end, run_id
                ) VALUES (%s,%s,%s)
                ON CONFLICT (pipeline_name) DO UPDATE
                SET current_window_end=GREATEST(
                        week6_control.log_report_watermark.current_window_end,
                        EXCLUDED.current_window_end
                    ),
                    run_id=CASE
                        WHEN EXCLUDED.current_window_end
                             > week6_control.log_report_watermark.current_window_end
                        THEN EXCLUDED.run_id
                        ELSE week6_control.log_report_watermark.run_id
                    END,
                    updated_at=CASE
                        WHEN EXCLUDED.current_window_end
                             > week6_control.log_report_watermark.current_window_end
                        THEN NOW()
                        ELSE week6_control.log_report_watermark.updated_at
                    END
                WHERE %s::timestamptz
                      <= week6_control.log_report_watermark.current_window_end
                """,
                (
                    LOG_REPORT_PIPELINE,
                    config["window_end"],
                    config["run_id"],
                    config["window_start"],
                ),
            )
    return {
        "minute_report_count": minute_count,
        "status_report_count": status_count,
    }
