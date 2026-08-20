from __future__ import annotations

from datetime import datetime, timezone

import pytest

from exercises.week6.log_contracts import (
    LogContractError,
    calculate_batch_ingestion_lag_seconds,
    calculate_ingestion_lag_seconds,
    floor_to_rotation,
    normalize_spark_timestamp_utc,
    raw_partition_path,
    resolve_log_report_configuration,
    select_stream_lineage_id,
    stream_batch_sequence_action,
    validate_log_event,
    validate_micro_batch_seconds,
    validate_stream_generation_id,
)


def test_rotation_is_stable_inside_five_minute_window() -> None:
    first = floor_to_rotation("2026-08-14T10:04:59Z")
    boundary = floor_to_rotation("2026-08-14T10:05:00Z")
    assert first.isoformat() == "2026-08-14T10:00:00+00:00"
    assert boundary.isoformat() == "2026-08-14T10:05:00+00:00"
    assert raw_partition_path("hdfs://namenode:9000/raw", first).endswith(
        "/ingest_date=2026-08-14/ingest_hour=10/rotation_5m=20260814T1000Z"
    )


@pytest.mark.parametrize("seconds", [1, 30, 60])
def test_micro_batch_contract_meets_one_minute_sla(seconds: int) -> None:
    assert validate_micro_batch_seconds(seconds) == seconds


@pytest.mark.parametrize("seconds", [0, 61, "invalid"])
def test_micro_batch_contract_rejects_out_of_sla(seconds) -> None:
    with pytest.raises(LogContractError):
        validate_micro_batch_seconds(seconds)


def test_log_validation_requires_operational_fields() -> None:
    valid = {
        "event_id": "evt-1",
        "event_time": "2026-08-14T10:00:00Z",
        "service": "checkout",
        "method": "POST",
        "path": "/orders",
        "status_code": 201,
        "latency_ms": 42,
    }
    assert validate_log_event(valid) == []
    invalid = {**valid, "event_time": None, "status_code": 700, "latency_ms": -1}
    errors = validate_log_event(invalid)
    assert any("event_time" in error for error in errors)
    assert any("status_code" in error for error in errors)
    assert any("latency_ms" in error for error in errors)


def test_report_window_supports_bounded_backfill() -> None:
    config = resolve_log_report_configuration(
        run_id="manual__backfill",
        conf={
            "window_start": "2026-08-14T09:00:00Z",
            "window_end": "2026-08-14T10:00:00Z",
        },
        data_interval_start=datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc),
        data_interval_end=datetime(2026, 8, 14, 10, 5, tzinfo=timezone.utc),
        reference_time=datetime(2026, 8, 14, 10, 3, tzinfo=timezone.utc),
    )
    assert config.mode == "backfill"
    assert config.window_start == "2026-08-14T09:00:00+00:00"


def test_scheduled_report_waits_for_late_event_settlement() -> None:
    config = resolve_log_report_configuration(
        run_id="scheduled__2026-08-14T10:10:00Z",
        conf={},
        data_interval_start=datetime(2026, 8, 14, 10, 5, tzinfo=timezone.utc),
        data_interval_end=datetime(2026, 8, 14, 10, 10, tzinfo=timezone.utc),
        settlement_seconds=180,
        max_event_delay_seconds=120,
        micro_batch_seconds=30,
    )
    # Event 10:04:59 đến trễ 120 giây vẫn kịp micro-batch trước lần chạy 10:10.
    assert config.window_start == "2026-08-14T10:00:00+00:00"
    assert config.window_end == "2026-08-14T10:05:00+00:00"
    assert config.data_available_through == "2026-08-14T10:05:00+00:00"


def test_report_rejects_settlement_shorter_than_lateness_plus_batch() -> None:
    with pytest.raises(LogContractError, match="settlement_seconds"):
        resolve_log_report_configuration(
            run_id="scheduled__unsafe",
            conf={},
            data_interval_start=datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc),
            data_interval_end=datetime(2026, 8, 14, 10, 5, tzinfo=timezone.utc),
            settlement_seconds=149,
            max_event_delay_seconds=120,
            micro_batch_seconds=30,
        )


def test_manual_report_rejects_window_that_is_not_settled() -> None:
    with pytest.raises(LogContractError, match="chưa qua settlement"):
        resolve_log_report_configuration(
            run_id="manual__too_early",
            conf={
                "window_start": "2026-08-14T09:55:00Z",
                "window_end": "2026-08-14T10:00:00Z",
            },
            data_interval_start=datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc),
            data_interval_end=datetime(2026, 8, 14, 10, 5, tzinfo=timezone.utc),
            reference_time=datetime(2026, 8, 14, 10, 2, 59, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize("value", ["generation-20260814-v2", "local.v1", "g_1"])
def test_stream_generation_id_is_path_and_key_safe(value: str) -> None:
    assert validate_stream_generation_id(value) == value


@pytest.mark.parametrize("value", ["", "has space", "/checkpoint", "a" * 81])
def test_stream_generation_id_rejects_ambiguous_values(value: str) -> None:
    with pytest.raises(LogContractError):
        validate_stream_generation_id(value)


def test_checkpoint_reset_is_rejected_inside_same_generation() -> None:
    with pytest.raises(LogContractError, match="quay lùi"):
        stream_batch_sequence_action(0, 63)
    assert stream_batch_sequence_action(63, 63, "success") == "replay"
    assert stream_batch_sequence_action(64, 63) == "process"


def test_generation_lineage_continues_only_for_exact_next_epoch() -> None:
    assert select_stream_lineage_id(
        stream_generation_id="local-v2",
        stream_batch_id=64,
        active_last_successful_batch_id=63,
        active_lineage_id="legacy-v1",
    ) == "legacy-v1"
    assert select_stream_lineage_id(
        stream_generation_id="local-v2",
        stream_batch_id=0,
        active_last_successful_batch_id=63,
        active_lineage_id="legacy-v1",
    ) == "local-v2"


def test_ingestion_lag_is_never_negative() -> None:
    assert calculate_ingestion_lag_seconds(
        "2026-08-14T10:00:00Z", "2026-08-14T10:00:42Z"
    ) == 42
    assert calculate_ingestion_lag_seconds(
        "2026-08-14T10:00:01Z", "2026-08-14T10:00:00Z"
    ) == 0


def test_batch_lag_uses_worst_event_instead_of_freshest_event() -> None:
    lag = calculate_batch_ingestion_lag_seconds(
        ["2026-08-14T09:58:30Z", "2026-08-14T10:00:00Z"],
        "2026-08-14T10:00:00Z",
    )
    assert lag == 90


def test_spark_naive_timestamp_is_interpreted_as_session_utc() -> None:
    normalized = normalize_spark_timestamp_utc(datetime(2026, 8, 14, 10, 0))
    assert normalized == datetime(2026, 8, 14, 10, 0, tzinfo=timezone.utc)
