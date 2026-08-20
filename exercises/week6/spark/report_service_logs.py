"""Tái lập report service log từ raw HDFS cho lịch chạy và backfill Airflow."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week6.log_repository import (
    prepare_log_report_staging,
    update_log_report_counts,
)
from exercises.week6.spark.transform_promotions import (
    DEFAULT_JDBC_PACKAGE,
    jdbc_configuration,
)


def aggregate_requests(logs: DataFrame, run_id: str) -> DataFrame:
    return (
        logs.withColumn("minute_start", F.date_trunc("minute", "event_time"))
        .groupBy("minute_start", "service")
        .agg(
            F.count("*").alias("request_count"),
            F.round(F.avg("latency_ms"), 3).alias("avg_latency_ms"),
            F.max("latency_ms").alias("max_latency_ms"),
        )
        .withColumn("run_id", F.lit(run_id))
        .select(
            "run_id",
            "minute_start",
            "service",
            "request_count",
            "avg_latency_ms",
            "max_latency_ms",
        )
    )


def aggregate_statuses(logs: DataFrame, run_id: str) -> DataFrame:
    counts = (
        logs.withColumn("minute_start", F.date_trunc("minute", "event_time"))
        .groupBy("minute_start", "service", "status_code")
        .agg(F.count("*").alias("request_count"))
    )
    grain = Window.partitionBy("minute_start", "service")
    return (
        counts.withColumn(
            "percentage",
            F.round(
                F.col("request_count") * F.lit(100.0)
                / F.sum("request_count").over(grain),
                3,
            ),
        )
        .withColumn("run_id", F.lit(run_id))
        .select(
            "run_id",
            "minute_start",
            "service",
            "status_code",
            "request_count",
            "percentage",
        )
    )


def read_window(
    spark: SparkSession,
    *,
    raw_path: str,
    window_start: str,
    window_end: str,
) -> DataFrame:
    base_path = raw_path.rstrip("/")
    globs = (
        f"{base_path}/ingest_date=*/ingest_hour=*/rotation_5m=*/"
        "stream_generation_id=*/stream_batch_id=*",
        f"{base_path}/ingest_date=*/ingest_hour=*/rotation_5m=*/stream_batch_id=*",
        f"{base_path}/stream_batch_id=*/ingest_date=*/ingest_hour=*/rotation_5m=*",
    )
    frames = []
    for glob in globs:
        jvm_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(glob)
        filesystem = jvm_path.getFileSystem(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        if filesystem.globStatus(jvm_path):
            frames.append(spark.read.option("basePath", base_path).parquet(glob))
    if not frames:
        raise RuntimeError(f"Không tìm thấy raw service log tại {base_path}")
    logs = frames[0]
    for frame in frames[1:]:
        logs = logs.unionByName(frame, allowMissingColumns=True)
    return (
        logs
        .filter(F.col("is_valid"))
        .filter(F.col("event_time") >= F.to_timestamp(F.lit(window_start)))
        .filter(F.col("event_time") < F.to_timestamp(F.lit(window_end)))
        .dropDuplicates(["event_id"])
    )


def build_spark(run_id: str) -> SparkSession:
    master_url = os.getenv("SPARK_MASTER_URL", "local[2]")
    builder = (
        SparkSession.builder.appName(f"week6-log-report-{run_id}")
        .master(master_url)
        .config(
            "spark.cores.max",
            os.getenv("WEEK6_LOG_REPORT_MAX_CORES", "1"),
        )
        .config("spark.hadoop.dfs.replication", os.getenv("HDFS_REPLICATION", "1"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("WEEK6_LOG_SHUFFLE_PARTITIONS", "4"))
        .config(
            "spark.jars.packages",
            os.getenv("WEEK6_JDBC_PACKAGE", DEFAULT_JDBC_PACKAGE),
        )
    )
    if master_url.startswith("spark://"):
        builder = builder.config(
            "spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler")
        ).config("spark.driver.bindAddress", "0.0.0.0")
    return builder.getOrCreate()


def run(
    *,
    run_id: str,
    window_start: str,
    window_end: str,
    raw_path: str,
    report_staging_path: str,
    summary_path: str | None = None,
) -> dict:
    prepare_log_report_staging(run_id)
    spark = build_spark(run_id)
    requests = statuses = logs = None
    try:
        logs = read_window(
            spark,
            raw_path=raw_path,
            window_start=window_start,
            window_end=window_end,
        ).cache()
        requests = aggregate_requests(logs, run_id).cache()
        statuses = aggregate_statuses(logs, run_id).cache()
        source_count = logs.count()
        minute_count = requests.count()
        status_count = statuses.count()

        database = DatabaseConfig.from_env()
        jdbc_url, jdbc_properties = jdbc_configuration(database)
        requests.write.mode("append").jdbc(
            jdbc_url,
            "week6_log.requests_per_minute_staging",
            properties=jdbc_properties,
        )
        statuses.write.mode("append").jdbc(
            jdbc_url,
            "week6_log.status_distribution_staging",
            properties=jdbc_properties,
        )

        output = report_staging_path.rstrip("/")
        requests.write.mode("overwrite").parquet(f"{output}/requests_per_minute")
        statuses.write.mode("overwrite").parquet(f"{output}/status_distribution")
        update_log_report_counts(
            run_id,
            source_count=source_count,
            minute_report_count=minute_count,
            status_report_count=status_count,
        )
        summary = {
            "run_id": run_id,
            "window_start": window_start,
            "window_end": window_end,
            "source_count": source_count,
            "minute_report_count": minute_count,
            "status_report_count": status_count,
            "report_staging_path": output,
        }
        if summary_path:
            path = Path(summary_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        return summary
    finally:
        for frame in (requests, statuses, logs):
            if frame is not None:
                frame.unpersist()
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Report/backfill log từ raw HDFS")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument(
        "--raw-path",
        default=os.getenv("WEEK6_LOG_RAW_PATH", "hdfs://namenode:9000/data/week6/raw/service-logs"),
    )
    parser.add_argument(
        "--report-staging-path",
        required=True,
        help="URI HDFS staging dành riêng cho run; chỉ Airflow publish sau DQ",
    )
    parser.add_argument("--summary-path")
    args = parser.parse_args()
    print(json.dumps(run(**vars(args)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
