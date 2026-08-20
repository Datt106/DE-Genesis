from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from exercises.week6.config import (
    ConfigurationError,
    align_scheduled_window_to_watermark,
    resolve_run_configuration,
)
from exercises.week6.ingestion import validate_week6_promotion


ROOT = Path(__file__).resolve().parents[3]


def test_scheduled_window_starts_at_successful_watermark_to_fill_gap() -> None:
    config = resolve_run_configuration(
        run_id="scheduled__2026-08-14",
        conf={},
        data_interval_start=datetime(2026, 8, 14, tzinfo=timezone.utc),
        data_interval_end=datetime(2026, 8, 15, tzinfo=timezone.utc),
    )
    aligned = align_scheduled_window_to_watermark(
        config, "2026-08-12T00:00:00Z"
    )
    assert aligned.window_start == "2026-08-12T00:00:00+00:00"
    assert aligned.window_end == "2026-08-15T00:00:00+00:00"


def test_watermark_cannot_create_empty_or_regressing_scheduled_window() -> None:
    config = resolve_run_configuration(
        run_id="scheduled__2026-08-14",
        conf={},
        data_interval_start=datetime(2026, 8, 14, tzinfo=timezone.utc),
        data_interval_end=datetime(2026, 8, 15, tzinfo=timezone.utc),
    )
    with pytest.raises(ConfigurationError):
        align_scheduled_window_to_watermark(config, "2026-08-15T00:00:00Z")


def test_promotion_pipeline_quality_precedes_atomic_publish() -> None:
    dag = (ROOT / "dags" / "de_genesis_week6_production_pipeline.py").read_text(
        encoding="utf-8"
    )
    assert dag.index('task_id="quality_gate_curated"') < dag.index(
        'task_id="publish_curated_snapshot"'
    )
    repository = (ROOT / "exercises" / "week6" / "repository.py").read_text(
        encoding="utf-8"
    )
    assert "pg_advisory_xact_lock" in repository
    assert "sales_promotion_staging" in repository
    assert "GREATEST(" in repository


def test_promotion_spark_uses_jdbc_and_filters_latest_active_state() -> None:
    source = (
        ROOT / "exercises" / "week6" / "spark" / "transform_promotions.py"
    ).read_text(encoding="utf-8")
    assert "spark.read.jdbc(" in source
    assert ".write.mode(\"append\").jdbc(" in source
    assert "WHERE status='active'" in source
    assert "DISTINCT ON (promotion_id)" in source
    assert "toLocalIterator" not in source
    assert "fetchall()" not in source


def test_week6_promotion_requires_status_and_updated_at() -> None:
    payload = {
        "promotion_id": "PROMO-1",
        "product_id": "PRODUCT-1",
        "discount_type": "percentage",
        "discount_value": 10,
        "starts_at": "2026-08-01T00:00:00Z",
        "ends_at": "2026-08-31T23:59:59Z",
        "version": 1,
    }
    errors = validate_week6_promotion(payload)
    assert "status không được để trống" in errors
    assert "updated_at không được để trống" in errors
    assert validate_week6_promotion(
        {
            **payload,
            "status": "inactive",
            "updated_at": "2026-08-14T00:00:00Z",
        }
    ) == []
