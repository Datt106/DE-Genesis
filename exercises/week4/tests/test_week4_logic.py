from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import random
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType


WEEK4_DIR = Path(__file__).resolve().parents[1]
if str(WEEK4_DIR) not in sys.path:
    sys.path.insert(0, str(WEEK4_DIR))

import kafka_producer
import spark_streaming_kafka


class _Metadata:
    partition = 0

    def __init__(self, offset: int) -> None:
        self.offset = offset


class _Future:
    def __init__(self, offset: int) -> None:
        self.offset = offset

    def get(self, timeout: float) -> _Metadata:
        assert timeout > 0
        return _Metadata(self.offset)


class _Producer:
    def __init__(self) -> None:
        self.values: list[dict] = []

    def send(self, topic: str, value: dict) -> _Future:
        assert topic == "test-topic"
        self.values.append(value)
        return _Future(len(self.values) - 1)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("de-genesis-week4-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_producer_sends_confirmed_deterministic_events() -> None:
    producer = _Producer()
    sent = kafka_producer.send_events(
        producer=producer,
        topic="test-topic",
        count=3,
        interval_seconds=0,
        rng=random.Random(7),
        sleep=lambda _: None,
    )

    assert sent == 3
    assert len(producer.values) == 3
    assert all(100 <= event["status_code"] <= 599 for event in producer.values)
    assert all(event["latency_ms"] >= 0 for event in producer.values)


def test_producer_can_inject_reproducible_quarantine_event() -> None:
    producer = _Producer()
    kafka_producer.send_events(
        producer=producer,
        topic="test-topic",
        count=5,
        interval_seconds=0,
        invalid_every=2,
        rng=random.Random(11),
        sleep=lambda _: None,
    )

    assert [event["latency_ms"] < 0 for event in producer.values] == [
        False,
        True,
        False,
        True,
        False,
    ]


def test_parse_filter_and_aggregate(spark: SparkSession) -> None:
    valid = {
        "ts": datetime(2026, 7, 12, 1, 0, tzinfo=timezone.utc).isoformat(),
        "service": "catalog",
        "method": "GET",
        "path": "/products",
        "status_code": 500,
        "latency_ms": 250,
    }
    invalid = {**valid, "latency_ms": -1}
    schema = StructType([StructField("value", BinaryType(), False)])
    raw = spark.createDataFrame(
        [
            (json.dumps(valid).encode("utf-8"),),
            (json.dumps(invalid).encode("utf-8"),),
            (b"not-json",),
        ],
        schema=schema,
    )

    logs = spark_streaming_kafka.parse_valid_logs(raw)
    assert logs.count() == 1

    result = spark_streaming_kafka.aggregate_logs(logs, "1 minute").first()
    assert result["service"] == "catalog"
    assert result["status_code"] == 500
    assert result["requests"] == 1
    assert result["avg_latency_ms"] == 250.0
    assert result["max_latency_ms"] == 250
    assert result["is_error"] is True


def test_classification_preserves_rejected_payload_and_reason(
    spark: SparkSession,
) -> None:
    valid = {
        "ts": "2026-07-12T01:00:00+00:00",
        "service": "catalog",
        "method": " get ",
        "path": "/products",
        "status_code": 200,
        "latency_ms": 25,
    }
    invalid_latency = {**valid, "latency_ms": -5}
    invalid_method = {**valid, "method": None}
    schema = StructType([StructField("value", BinaryType(), False)])
    raw = spark.createDataFrame(
        [
            (json.dumps(valid).encode("utf-8"),),
            (json.dumps(invalid_latency).encode("utf-8"),),
            (json.dumps(invalid_method).encode("utf-8"),),
            (b"not-json",),
        ],
        schema=schema,
    )

    classified = spark_streaming_kafka.classify_logs(raw)
    rows = classified.select(
        "raw_json",
        "method",
        "is_valid",
        "rejection_reason",
    ).collect()

    assert sum(row["is_valid"] for row in rows) == 1
    assert next(row for row in rows if row["is_valid"])["method"] == "GET"
    assert {row["rejection_reason"] for row in rows if not row["is_valid"]} == {
        "invalid_latency_ms",
        "invalid_method",
        "malformed_json",
    }

    quarantine = spark_streaming_kafka.rejected_logs(classified)
    assert quarantine.count() == 3
    assert "raw_json" in quarantine.columns
    assert "source_offset" in quarantine.columns


def test_quality_metrics_are_idempotent_per_batch(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    event = {
        "ts": "2026-07-12T01:00:00+00:00",
        "service": "payment",
        "method": "POST",
        "path": "/payments",
        "status_code": 201,
        "latency_ms": 40,
    }
    invalid = {**event, "status_code": 700}
    schema = StructType([StructField("value", BinaryType(), False)])
    raw = spark.createDataFrame(
        [
            (json.dumps(event).encode("utf-8"),),
            (json.dumps(invalid).encode("utf-8"),),
        ],
        schema=schema,
    )
    classified = spark_streaming_kafka.classify_logs(raw)
    metrics_root = tmp_path / "quality_metrics"

    spark_streaming_kafka.write_quality_metrics(
        classified,
        batch_id=7,
        output_path=str(metrics_root),
    )
    spark_streaming_kafka.write_quality_metrics(
        classified,
        batch_id=7,
        output_path=str(metrics_root),
    )

    batch_path = metrics_root / "batch_00000000000000000007"
    row = spark.read.parquet(str(batch_path)).first()
    assert spark.read.parquet(str(batch_path)).count() == 1
    assert row["batch_id"] == 7
    assert row["processed_records"] == 2
    assert row["accepted_records"] == 1
    assert row["rejected_records"] == 1
    assert row["rejection_ratio"] == 0.5


def test_sink_paths_must_be_distinct() -> None:
    with pytest.raises(ValueError, match="phải dùng đường dẫn khác nhau"):
        spark_streaming_kafka.validate_output_paths(
            {
                "--output": "/workspace/output/week4/same",
                "--checkpoint": "/workspace/output/week4/same/",
            }
        )

    with pytest.raises(ValueError, match="không được lồng đường dẫn"):
        spark_streaming_kafka.validate_output_paths(
            {
                "--output": "/workspace/output/week4/report",
                "--checkpoint": "/workspace/output/week4/report/checkpoint",
            }
        )
