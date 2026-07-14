from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession


WEEK3_DIR = Path(__file__).resolve().parents[1]
if str(WEEK3_DIR) not in sys.path:
    sys.path.insert(0, str(WEEK3_DIR))

import spark_batch
import generate_olist_1gb
import olist_format_benchmark


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("de-genesis-week3-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def raw_row(*, order_id: str, quantity: str, unit_price: str) -> tuple[str, ...]:
    return (
        order_id,
        "2026-06-01",
        "101",
        "An",
        "501",
        "Laptop",
        "Electronics",
        quantity,
        unit_price,
        "Ha Noi",
        None,
    )


def test_quality_split_and_aggregate(spark: SparkSession) -> None:
    valid_source = raw_row(order_id="1", quantity="2", unit_price="10.00")
    invalid_source = raw_row(order_id="2", quantity="0", unit_price="5.00")
    raw = spark.createDataFrame(
        [valid_source, valid_source, invalid_source],
        schema=spark_batch.RAW_SCHEMA,
    ).withColumn("_source_file", spark_batch.F.lit("test.csv"))

    valid, rejected, duplicates_removed = spark_batch.normalize_and_validate(raw)

    assert duplicates_removed == 1
    assert valid.count() == 1
    assert rejected.count() == 1
    assert "quantity phải > 0" in rejected.first()["rejection_reason"]
    assert valid.first()["line_amount"] == Decimal("20.00")

    category = spark_batch.build_aggregates(valid)["category_summary"].first()
    assert category["orders"] == 1
    assert category["units_sold"] == 2
    assert category["revenue"] == Decimal("20.00")


def test_join_spark_path_preserves_uri() -> None:
    assert (
        spark_batch.join_spark_path("hdfs://namenode:9000/data/", "/raw/sales.csv")
        == "hdfs://namenode:9000/data/raw/sales.csv"
    )


def test_replica_estimate_has_safety_margin() -> None:
    copies = generate_olist_1gb.estimate_replica_count(
        base_rows=100,
        average_source_row_bytes=100.0,
        target_bytes=20_000,
    )
    assert copies >= 2


def small_olist_inputs(spark: SparkSession) -> dict[str, object]:
    timestamp = datetime(2024, 1, 2, 10, 30, 0)
    return {
        "items": spark.createDataFrame(
            [
                (
                    0,
                    "0:order-1:1",
                    "order-1",
                    1,
                    "product-1",
                    "seller-1",
                    timestamp,
                    Decimal("100.00"),
                    Decimal("15.00"),
                )
            ],
            olist_format_benchmark.LARGE_ITEM_SCHEMA,
        ),
        "orders": spark.createDataFrame(
            [
                (
                    "order-1",
                    "customer-1",
                    "delivered",
                    timestamp,
                    timestamp,
                    timestamp,
                    timestamp,
                    timestamp,
                )
            ],
            olist_format_benchmark.ORDER_SCHEMA,
        ),
        "customers": spark.createDataFrame(
            [("customer-1", "unique-1", "10000", "ha noi", "HN")],
            olist_format_benchmark.CUSTOMER_SCHEMA,
        ),
        "products": spark.createDataFrame(
            [("product-1", "moveis", 10, 20, 1, 100, 10, 10, 10)],
            olist_format_benchmark.PRODUCT_SCHEMA,
        ),
        "sellers": spark.createDataFrame(
            [("seller-1", "10000", "ha noi", "HN")],
            olist_format_benchmark.SELLER_SCHEMA,
        ),
        "categories": spark.createDataFrame(
            [("moveis", "furniture")],
            olist_format_benchmark.CATEGORY_SCHEMA,
        ),
    }


def test_olist_join_and_three_spark_apis_are_equivalent(
    spark: SparkSession,
) -> None:
    joined = olist_format_benchmark.build_joined(small_olist_inputs(spark))
    quality = olist_format_benchmark.collect_join_quality(joined)
    assert quality == {
        "source_rows": 1,
        "unmatched_orders": 0,
        "unmatched_customers": 0,
        "unmatched_products": 0,
        "unmatched_sellers": 0,
    }

    curated = olist_format_benchmark.build_curated(joined)
    row = curated.first()
    assert row["category_english"] == "furniture"
    assert row["gross_amount"] == Decimal("115.00")
    assert row["purchase_year"] == 2024

    dataframe_summary = olist_format_benchmark.build_dataframe_category_summary(
        curated
    )
    sql_summary = olist_format_benchmark.build_sql_category_summary(curated)
    rdd_summary = olist_format_benchmark.build_rdd_category_summary(curated)
    assert olist_format_benchmark.assert_api_equivalence(
        dataframe_summary, sql_summary, rdd_summary
    ) == {"dataframe_sql_equal": True, "dataframe_rdd_equal": True}


def test_benchmark_workloads_have_stable_checksums(spark: SparkSession) -> None:
    curated = olist_format_benchmark.build_curated(
        olist_format_benchmark.build_joined(small_olist_inputs(spark))
    )
    full = olist_format_benchmark.execute_workload(
        curated, "full_scan_aggregate", 2024
    )
    filtered = olist_format_benchmark.execute_workload(
        curated, "filter_group_aggregate", 2024
    )
    assert full[0] == 1
    assert filtered[0] == 1
    assert len(full[1]) == 64
    assert "HashAggregate" in full[3] or "ObjectHashAggregate" in full[3]


def test_three_format_round_trip_and_success_marker(
    spark: SparkSession, tmp_path: Path
) -> None:
    curated = olist_format_benchmark.build_curated(
        olist_format_benchmark.build_joined(small_olist_inputs(spark))
    ).coalesce(1)
    output = (tmp_path / "week3-roundtrip").as_posix()
    paths, write_seconds = olist_format_benchmark.write_formats(
        curated, output, "overwrite"
    )

    format_metrics: dict[str, dict[str, object]] = {}
    for data_format in olist_format_benchmark.FORMAT_ORDER:
        restored = olist_format_benchmark.read_format(
            spark, data_format, paths[data_format]
        )
        assert restored.count() == 1
        metric: dict[str, object] = olist_format_benchmark.data_file_metrics(
            spark, paths[data_format]
        )
        metric.update(
            {
                "format": data_format,
                "compression": "none" if data_format == "csv" else "snappy",
                "path": paths[data_format],
                "write_seconds": write_seconds[data_format],
            }
        )
        format_metrics[data_format] = metric

    trials, plans = olist_format_benchmark.run_benchmark(
        spark,
        paths,
        benchmark_year=2024,
        warmups=0,
        trials=1,
        seed=42,
    )
    assert len(trials) == 6
    assert len(plans) == 6
    assert olist_format_benchmark.validate_benchmark(trials)
    summaries = olist_format_benchmark.summarize_benchmark(
        trials, format_metrics
    )

    quality: dict[str, object] = {
        "generated_at_utc": "2026-07-14T00:00:00+00:00",
        "application_id": "pytest-local",
        "measured_trials": 6,
        "status_for_test": "valid",
    }
    olist_format_benchmark.write_reports(
        spark,
        output,
        "overwrite",
        quality=quality,
        format_metrics=format_metrics,
        trials=trials,
        summaries=summaries,
        plans=plans,
    )
    status = spark.read.json(
        olist_format_benchmark.join_spark_path(output, "run_status")
    ).first()
    assert status["status"] == "success"
    assert status["measured_trials"] == 6

    olist_format_benchmark.clear_previous_reports(spark, output)
    assert not (tmp_path / "week3-roundtrip" / "run_status").exists()
    assert (tmp_path / "week3-roundtrip" / "curated_parquet").exists()
