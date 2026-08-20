"""Kafka -> Spark Structured Streaming -> HDFS/PostgreSQL cho service logs."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week6.log_contracts import (
    normalize_spark_timestamp_utc,
    validate_stream_generation_id,
    validate_micro_batch_seconds,
)
from exercises.week6.log_repository import (
    fail_stream_batch,
    prepare_stream_batch,
    publish_stream_batch,
)
from exercises.week6.spark.transform_promotions import jdbc_configuration


QUERY_NAME = "de_genesis_week6_service_logs"
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
DEFAULT_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"

LOG_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_time", StringType()),
        StructField("service", StringType()),
        StructField("method", StringType()),
        StructField("path", StringType()),
        StructField("status_code", IntegerType()),
        StructField("latency_ms", IntegerType()),
        StructField("host", StringType()),
    ]
)


def parse_logs(raw: DataFrame, max_event_delay_seconds: int) -> DataFrame:
    """Giữ payload raw, Kafka lineage và lý do loại record trong cùng dataset."""

    parsed = (
        raw.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("log", F.from_json("raw_json", LOG_SCHEMA))
        .select(
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "log.*",
        )
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("event_id", F.trim("event_id"))
        .withColumn("service", F.trim("service"))
        .withColumn("method", F.upper(F.trim("method")))
        .withColumn("path", F.trim("path"))
        .withColumn("host", F.trim("host"))
        .withColumn("landed_at", F.current_timestamp())
    )
    required = (
        (F.length(F.col("event_id")) > 0)
        & F.col("event_time").isNotNull()
        & (F.length(F.col("service")) > 0)
        & F.col("method").isin("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")
        & (F.length(F.col("path")) > 0)
        & F.col("status_code").between(100, 599)
        & F.col("latency_ms").isNotNull()
        & (F.col("latency_ms") >= 0)
    )
    not_too_late = F.col("event_time") >= F.expr(
        f"landed_at - INTERVAL {max_event_delay_seconds} SECONDS"
    )
    return (
        parsed.withColumn(
            "is_valid", F.coalesce(required & not_too_late, F.lit(False))
        )
        .withColumn(
            "validation_error",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("event_id").isNull() | (F.length("event_id") == 0),
                    F.lit("event_id bắt buộc"),
                ),
                F.when(F.col("event_time").isNull(), F.lit("event_time không hợp lệ")),
                F.when(
                    F.col("service").isNull() | (F.length("service") == 0),
                    F.lit("service bắt buộc"),
                ),
                F.when(
                    F.col("method").isNull()
                    | (F.length("method") == 0)
                    | ~F.col("method").isin(
                        "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"
                    ),
                    F.lit("method không hợp lệ"),
                ),
                F.when(
                    F.col("path").isNull() | (F.length("path") == 0),
                    F.lit("path bắt buộc"),
                ),
                F.when(
                    F.col("status_code").isNull()
                    | ~F.col("status_code").between(100, 599),
                    F.lit("status_code không hợp lệ"),
                ),
                F.when(
                    F.col("latency_ms").isNull(), F.lit("latency_ms bắt buộc")
                ),
                F.when(F.col("latency_ms") < 0, F.lit("latency_ms âm")),
                F.when(~not_too_late, F.lit("event đến quá watermark cho phép")),
            ),
        )
        .withColumn(
            "rotation_time",
            F.coalesce("kafka_timestamp", "event_time", "landed_at"),
        )
        .withColumn("ingest_date", F.date_format("rotation_time", "yyyy-MM-dd"))
        .withColumn("ingest_hour", F.date_format("rotation_time", "HH"))
        .withColumn(
            "rotation_5m",
            F.date_format(
                F.from_unixtime(
                    F.floor(F.unix_timestamp("rotation_time") / F.lit(300)) * F.lit(300)
                ),
                "yyyyMMdd'T'HHmm'Z'",
            ),
        )
    )


def request_report(
    valid: DataFrame, stream_generation_id: str, stream_batch_id: int
) -> DataFrame:
    return (
        valid.withColumn("minute_start", F.date_trunc("minute", "event_time"))
        .groupBy("minute_start", "service")
        .agg(
            F.count("*").alias("request_count"),
            F.sum("latency_ms").cast("decimal(20,3)").alias("latency_sum_ms"),
            F.max("latency_ms").alias("max_latency_ms"),
        )
        .withColumn("stream_generation_id", F.lit(stream_generation_id))
        .withColumn("stream_batch_id", F.lit(stream_batch_id).cast("long"))
        .select(
            "stream_generation_id",
            "stream_batch_id",
            "minute_start",
            "service",
            "request_count",
            "latency_sum_ms",
            "max_latency_ms",
        )
    )


def status_report(
    valid: DataFrame, stream_generation_id: str, stream_batch_id: int
) -> DataFrame:
    return (
        valid.withColumn("minute_start", F.date_trunc("minute", "event_time"))
        .groupBy("minute_start", "service", "status_code")
        .agg(F.count("*").alias("request_count"))
        .withColumn("stream_generation_id", F.lit(stream_generation_id))
        .withColumn("stream_batch_id", F.lit(stream_batch_id).cast("long"))
        .select(
            "stream_generation_id",
            "stream_batch_id",
            "minute_start",
            "service",
            "status_code",
            "request_count",
        )
    )


def process_batch(
    batch: DataFrame,
    stream_batch_id: int,
    *,
    raw_base_path: str,
    report_base_path: str,
    stream_generation_id: str,
    checkpoint_path: str,
    query_name: str = QUERY_NAME,
) -> None:
    """Ghi một epoch idempotent rồi publish contribution nguyên tử."""

    should_process = prepare_stream_batch(
        stream_generation_id,
        stream_batch_id,
        query_name,
        checkpoint_path,
    )
    if not should_process:
        return
    batch = batch.cache()
    valid = requests = statuses = None
    published = False
    try:
        stats = batch.agg(
            F.count("*").alias("raw_count"),
            F.sum(F.col("is_valid").cast("long")).alias("valid_count"),
            F.max("event_time").alias("max_event_time"),
            F.min(F.when(F.col("is_valid"), F.col("event_time"))).alias(
                "oldest_valid_event_time"
            ),
        ).first()
        raw_count = int(stats["raw_count"] or 0)
        valid_count = int(stats["valid_count"] or 0)
        invalid_count = raw_count - valid_count

        raw_with_epoch = batch.withColumn(
            "stream_generation_id", F.lit(stream_generation_id)
        ).withColumn(
            "stream_batch_id", F.lit(stream_batch_id).cast("long")
        )
        batch.sparkSession.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        (
            raw_with_epoch.write.mode("overwrite")
            .partitionBy(
                "ingest_date",
                "ingest_hour",
                "rotation_5m",
                "stream_generation_id",
                "stream_batch_id"
            )
            .parquet(raw_base_path.rstrip("/"))
        )
        raw_committed_at = datetime.now(timezone.utc)

        valid = batch.filter("is_valid").cache()
        requests = request_report(
            valid, stream_generation_id, stream_batch_id
        ).cache()
        statuses = status_report(
            valid, stream_generation_id, stream_batch_id
        ).cache()
        database = DatabaseConfig.from_env()
        jdbc_url, jdbc_properties = jdbc_configuration(database)
        requests.write.mode("append").jdbc(
            jdbc_url,
            "week6_log.requests_per_minute_stream_staging",
            properties=jdbc_properties,
        )
        statuses.write.mode("append").jdbc(
            jdbc_url,
            "week6_log.status_distribution_stream_staging",
            properties=jdbc_properties,
        )

        report_path = (
            f"{report_base_path.rstrip('/')}"
            f"/stream_generation_id={stream_generation_id}"
            f"/stream_batch_id={stream_batch_id}"
        )
        (
            requests.withColumn(
                "avg_latency_ms",
                F.round(F.col("latency_sum_ms") / F.col("request_count"), 3),
            )
            .write.mode("overwrite")
            .parquet(f"{report_path}/requests_per_minute")
        )
        statuses.write.mode("overwrite").parquet(
            f"{report_path}/status_distribution"
        )

        max_event_time = normalize_spark_timestamp_utc(stats["max_event_time"])
        oldest_valid_event_time = normalize_spark_timestamp_utc(
            stats["oldest_valid_event_time"]
        )
        lag = (
            max(0.0, (raw_committed_at - oldest_valid_event_time).total_seconds())
            if oldest_valid_event_time is not None
            else None
        )
        publish_stream_batch(
            stream_generation_id=stream_generation_id,
            stream_batch_id=stream_batch_id,
            query_name=query_name,
            raw_count=raw_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            max_event_time=max_event_time,
            ingestion_lag_seconds=lag,
        )
        published = True
    except Exception as exc:
        if not published:
            fail_stream_batch(
                stream_generation_id, stream_batch_id, query_name, str(exc)
            )
        raise
    finally:
        for frame in (requests, statuses, valid, batch):
            if frame is not None:
                try:
                    frame.unpersist()
                except Exception:
                    # Cleanup không được đổi telemetry success đã commit thành failed.
                    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Streaming service log production-like")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    )
    parser.add_argument("--topic", default=os.getenv("WEEK6_LOG_TOPIC", "week6-service-logs"))
    parser.add_argument(
        "--checkpoint",
        default=os.getenv(
            "WEEK6_LOG_CHECKPOINT", "hdfs://namenode:9000/data/week6/checkpoints/service-logs"
        ),
    )
    parser.add_argument(
        "--generation-id",
        default=os.getenv("WEEK6_LOG_GENERATION_ID", "local-v1"),
    )
    parser.add_argument(
        "--raw-path",
        default=os.getenv("WEEK6_LOG_RAW_PATH", "hdfs://namenode:9000/data/week6/raw/service-logs"),
    )
    parser.add_argument(
        "--report-path",
        default=os.getenv("WEEK6_LOG_REPORT_PATH", "hdfs://namenode:9000/data/week6/reports/live"),
    )
    parser.add_argument(
        "--micro-batch-seconds",
        type=int,
        default=int(os.getenv("WEEK6_LOG_MICRO_BATCH_SECONDS", "30")),
    )
    parser.add_argument(
        "--max-event-delay-seconds",
        type=int,
        default=int(os.getenv("WEEK6_LOG_MAX_EVENT_DELAY_SECONDS", "120")),
    )
    return parser


def build_spark() -> SparkSession:
    packages = os.getenv(
        "WEEK6_LOG_SPARK_PACKAGES",
        f"{DEFAULT_KAFKA_PACKAGE},{DEFAULT_JDBC_PACKAGE}",
    )
    master_url = os.getenv("SPARK_MASTER_URL", "local[2]")
    builder = (
        SparkSession.builder.appName(QUERY_NAME)
        .master(master_url)
        .config(
            "spark.cores.max",
            os.getenv("WEEK6_LOG_STREAM_MAX_CORES", "1"),
        )
        .config("spark.hadoop.dfs.replication", os.getenv("HDFS_REPLICATION", "1"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("WEEK6_LOG_SHUFFLE_PARTITIONS", "4"))
        .config("spark.jars.packages", packages)
    )
    if master_url.startswith("spark://"):
        builder = builder.config(
            "spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "week6-log-stream")
        ).config("spark.driver.bindAddress", "0.0.0.0")
    return builder.getOrCreate()


def main() -> int:
    args = build_parser().parse_args()
    micro_batch_seconds = validate_micro_batch_seconds(args.micro_batch_seconds)
    stream_generation_id = validate_stream_generation_id(args.generation_id)
    if args.max_event_delay_seconds <= 0:
        raise SystemExit("--max-event-delay-seconds phải lớn hơn 0")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", os.getenv("WEEK6_LOG_STARTING_OFFSETS", "earliest"))
            .option("failOnDataLoss", "true")
            .load()
        )
        parsed = parse_logs(kafka, args.max_event_delay_seconds).withWatermark(
            "event_time", f"{args.max_event_delay_seconds} seconds"
        )
        query = (
            parsed.writeStream.foreachBatch(
                lambda frame, epoch: process_batch(
                    frame,
                    epoch,
                    raw_base_path=args.raw_path,
                    report_base_path=args.report_path,
                    stream_generation_id=stream_generation_id,
                    checkpoint_path=args.checkpoint,
                )
            )
            .option("checkpointLocation", args.checkpoint)
            .queryName(QUERY_NAME)
            .trigger(processingTime=f"{micro_batch_seconds} seconds")
            .start()
        )
        query.awaitTermination()
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
