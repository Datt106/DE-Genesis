"""Đọc log từ Kafka và tổng hợp bằng Spark Structured Streaming."""

from __future__ import annotations

import argparse
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "service-logs")
CHECKPOINT = os.getenv("WEEK4_CHECKPOINT", "/workspace/output/week4/checkpoint")
OUTPUT = os.getenv("WEEK4_OUTPUT", "/workspace/output/week4/status_report")
QUARANTINE_CHECKPOINT = os.getenv(
    "WEEK4_QUARANTINE_CHECKPOINT",
    "/workspace/output/week4/checkpoint_quarantine",
)
QUARANTINE_OUTPUT = os.getenv(
    "WEEK4_QUARANTINE_OUTPUT",
    "/workspace/output/week4/quarantine",
)
METRICS_CHECKPOINT = os.getenv(
    "WEEK4_METRICS_CHECKPOINT",
    "/workspace/output/week4/checkpoint_metrics",
)
METRICS_OUTPUT = os.getenv(
    "WEEK4_METRICS_OUTPUT",
    "/workspace/output/week4/quality_metrics",
)


LOG_SCHEMA = StructType(
    [
        StructField("ts", StringType()),
        StructField("service", StringType()),
        StructField("method", StringType()),
        StructField("path", StringType()),
        StructField("status_code", IntegerType()),
        StructField("latency_ms", IntegerType()),
    ]
)


def _optional_source_column(
    raw: DataFrame,
    source_name: str,
    target_name: str,
    data_type: str,
):
    """Đọc metadata Kafka nếu có; fixture unit test chỉ bắt buộc cột value."""

    if source_name in raw.columns:
        return F.col(source_name).cast(data_type).alias(target_name)
    return F.lit(None).cast(data_type).alias(target_name)


def classify_logs(raw: DataFrame) -> DataFrame:
    """Giải mã và gắn đúng một lý do lỗi ưu tiên cho mỗi sự kiện."""

    parsed = (
        raw.select(
            F.col("value").cast("string").alias("raw_json"),
            _optional_source_column(raw, "topic", "source_topic", "string"),
            _optional_source_column(raw, "partition", "source_partition", "int"),
            _optional_source_column(raw, "offset", "source_offset", "long"),
            _optional_source_column(
                raw,
                "timestamp",
                "kafka_timestamp",
                "timestamp",
            ),
        )
        .withColumn("log", F.from_json(F.col("raw_json"), LOG_SCHEMA))
        .select(
            "raw_json",
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_timestamp",
            "log.*",
        )
        .withColumn("service", F.trim("service"))
        .withColumn("method", F.upper(F.trim("method")))
        .withColumn("path", F.trim("path"))
        .withColumn("event_time", F.to_timestamp("ts"))
    )

    rejection_reason = (
        F.when(
            F.col("raw_json").isNull()
            | F.get_json_object(F.col("raw_json"), "$").isNull(),
            F.lit("malformed_json"),
        )
        .when(F.col("event_time").isNull(), F.lit("invalid_or_missing_ts"))
        .when(
            F.col("service").isNull() | (F.length("service") == 0),
            F.lit("missing_service"),
        )
        .when(
            F.col("method").isNull()
            | ~F.col("method").isin("GET", "POST", "PUT", "PATCH", "DELETE"),
            F.lit("invalid_method"),
        )
        .when(
            F.col("path").isNull() | (F.length("path") == 0),
            F.lit("missing_path"),
        )
        .when(
            F.col("status_code").isNull()
            | ~F.col("status_code").between(100, 599),
            F.lit("invalid_status_code"),
        )
        .when(
            F.col("latency_ms").isNull() | (F.col("latency_ms") < 0),
            F.lit("invalid_latency_ms"),
        )
    )

    return parsed.withColumn("rejection_reason", rejection_reason).withColumn(
        "is_valid",
        F.col("rejection_reason").isNull(),
    )


def valid_logs(classified: DataFrame) -> DataFrame:
    """Chọn các sự kiện đủ điều kiện để aggregate."""

    return classified.filter(F.col("is_valid")).drop(
        "is_valid",
        "rejection_reason",
    )


def parse_valid_logs(raw: DataFrame) -> DataFrame:
    """API tương thích: giải mã và chỉ trả các sự kiện hợp lệ."""

    return valid_logs(classify_logs(raw))


def rejected_logs(classified: DataFrame) -> DataFrame:
    """Tạo bản ghi quarantine có payload, lý do và vị trí Kafka để truy vết."""

    return classified.filter(~F.col("is_valid")).select(
        "raw_json",
        "rejection_reason",
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("quarantined_at"),
    )


def summarize_quality(classified: DataFrame) -> DataFrame:
    """Tổng hợp accepted/rejected trong một micro-batch bằng một lần aggregate."""

    summary = classified.agg(
        F.count("*").cast("long").alias("processed_records"),
        F.coalesce(
            F.sum(F.when(F.col("is_valid"), 1).otherwise(0)),
            F.lit(0),
        )
        .cast("long")
        .alias("accepted_records"),
        F.coalesce(
            F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)),
            F.lit(0),
        )
        .cast("long")
        .alias("rejected_records"),
    )
    return summary.withColumn(
        "rejection_ratio",
        F.when(
            F.col("processed_records") > 0,
            F.round(
                F.col("rejected_records") / F.col("processed_records"),
                6,
            ),
        ).otherwise(F.lit(0.0)),
    )


def write_quality_metrics(
    classified_batch: DataFrame,
    batch_id: int,
    output_path: str,
) -> None:
    """Ghi idempotent một dòng metric Parquet cho micro-batch đã xử lý."""

    base_path = output_path.rstrip("/\\")
    batch_path = f"{base_path}/batch_{batch_id:020d}"
    (
        summarize_quality(classified_batch)
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("recorded_at", F.current_timestamp())
        .select(
            "batch_id",
            "recorded_at",
            "processed_records",
            "accepted_records",
            "rejected_records",
            "rejection_ratio",
        )
        .write.mode("overwrite")
        .parquet(batch_path)
    )


def validate_output_paths(paths: dict[str, str]) -> None:
    """Không cho hai sink/checkpoint dùng chung đường dẫn gây hỏng state."""

    normalized: dict[str, str] = {}
    for name, value in paths.items():
        path = value.replace("\\", "/").rstrip("/")
        if not path:
            raise ValueError(f"{name} không được để trống")
        for existing_path, existing_name in normalized.items():
            if path == existing_path:
                raise ValueError(
                    f"{name} và {existing_name} phải dùng đường dẫn khác nhau"
                )
            if path.startswith(existing_path + "/") or existing_path.startswith(
                path + "/"
            ):
                raise ValueError(
                    f"{name} và {existing_name} không được lồng đường dẫn"
                )
        normalized[path] = name


def aggregate_logs(logs: DataFrame, window_duration: str) -> DataFrame:
    """Tổng hợp số request, độ trễ và tỷ lệ lỗi theo cửa sổ sự kiện."""

    return (
        logs.groupBy(
            F.window("event_time", window_duration),
            "service",
            "status_code",
        )
        .agg(
            F.count("*").alias("requests"),
            F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
            F.max("latency_ms").alias("max_latency_ms"),
        )
        .withColumn(
            "is_error",
            F.when(F.col("status_code") >= 500, F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
    )


def build_streaming_report(
    logs: DataFrame,
    window_duration: str,
    watermark_delay: str,
) -> DataFrame:
    """Gắn watermark trước khi tạo phép tổng hợp có trạng thái."""

    return aggregate_logs(
        logs.withWatermark("event_time", watermark_delay),
        window_duration,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tổng hợp log Kafka bằng Spark Structured Streaming."
    )
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--checkpoint", default=CHECKPOINT)
    parser.add_argument("--output", default=OUTPUT)
    parser.add_argument("--quarantine-checkpoint", default=QUARANTINE_CHECKPOINT)
    parser.add_argument("--quarantine-output", default=QUARANTINE_OUTPUT)
    parser.add_argument("--metrics-checkpoint", default=METRICS_CHECKPOINT)
    parser.add_argument("--metrics-output", default=METRICS_OUTPUT)
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
        help="Chỉ áp dụng khi checkpoint chưa tồn tại.",
    )
    parser.add_argument(
        "--window-duration",
        default=os.getenv("WEEK4_WINDOW_DURATION", "1 minute"),
    )
    parser.add_argument(
        "--watermark-delay",
        default=os.getenv("WEEK4_WATERMARK_DELAY", "1 minute"),
    )
    parser.add_argument(
        "--trigger-seconds",
        type=float,
        default=float(os.getenv("WEEK4_TRIGGER_SECONDS", "10")),
        help="Chu kỳ micro-batch khi chạy liên tục.",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Xử lý hết dữ liệu đang có rồi tự kết thúc.",
    )
    parser.add_argument(
        "--stop-after-seconds",
        type=float,
        default=None,
        help="Tự dừng sau số giây chỉ định; bỏ trống để chạy liên tục.",
    )
    parser.add_argument(
        "--allow-data-loss",
        action="store_true",
        help=(
            "Cho phép Kafka offset bị thiếu; mặc định job dừng để không che giấu "
            "mất dữ liệu. Chỉ dùng khi phục hồi lab có chủ đích."
        ),
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.trigger_seconds <= 0:
        raise SystemExit("--trigger-seconds phải lớn hơn 0")
    if args.stop_after_seconds is not None and args.stop_after_seconds <= 0:
        raise SystemExit("--stop-after-seconds phải lớn hơn 0")
    if args.available_now and args.stop_after_seconds is not None:
        raise SystemExit(
            "Không dùng đồng thời --available-now và --stop-after-seconds"
        )
    try:
        validate_output_paths(
            {
                "--output": args.output,
                "--checkpoint": args.checkpoint,
                "--quarantine-output": args.quarantine_output,
                "--quarantine-checkpoint": args.quarantine_checkpoint,
                "--metrics-output": args.metrics_output,
                "--metrics-checkpoint": args.metrics_checkpoint,
            }
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    spark = (
        SparkSession.builder.appName("de-genesis-week4-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", str(not args.allow_data_loss).lower())
            .load()
        )
        classified = classify_logs(raw)
        logs = valid_logs(classified)
        quarantine = rejected_logs(classified)
        report = build_streaming_report(
            logs,
            window_duration=args.window_duration,
            watermark_delay=args.watermark_delay,
        )

        report_writer = (
            report.writeStream.outputMode("append")
            .format("parquet")
            .option("path", args.output)
            .option("checkpointLocation", args.checkpoint)
            .queryName("week4_status_report")
        )
        quarantine_writer = (
            quarantine.writeStream.outputMode("append")
            .format("parquet")
            .option("path", args.quarantine_output)
            .option("checkpointLocation", args.quarantine_checkpoint)
            .queryName("week4_quarantine")
        )

        def write_metrics(batch: DataFrame, batch_id: int) -> None:
            write_quality_metrics(batch, batch_id, args.metrics_output)

        metrics_writer = (
            classified.writeStream.outputMode("append")
            .option("checkpointLocation", args.metrics_checkpoint)
            .queryName("week4_quality_metrics")
            .foreachBatch(write_metrics)
        )
        if args.available_now:
            report_writer = report_writer.trigger(availableNow=True)
            quarantine_writer = quarantine_writer.trigger(availableNow=True)
            metrics_writer = metrics_writer.trigger(availableNow=True)
        else:
            trigger = {"processingTime": f"{args.trigger_seconds} seconds"}
            report_writer = report_writer.trigger(**trigger)
            quarantine_writer = quarantine_writer.trigger(**trigger)
            metrics_writer = metrics_writer.trigger(**trigger)

        print(
            "Bắt đầu streaming: "
            f"topic={args.topic}, broker={args.bootstrap_servers}, "
            f"output={args.output}, checkpoint={args.checkpoint}, "
            f"quarantine={args.quarantine_output}, "
            f"metrics={args.metrics_output}, "
            f"window={args.window_duration}, watermark={args.watermark_delay}",
            flush=True,
        )
        queries = [
            report_writer.start(),
            quarantine_writer.start(),
            metrics_writer.start(),
        ]
        if args.available_now:
            for query in queries:
                query.awaitTermination()
        elif args.stop_after_seconds is None:
            spark.streams.awaitAnyTermination()
        else:
            spark.streams.awaitAnyTermination(args.stop_after_seconds)
            print("Đã hết thời gian chạy, đang dừng các streaming query.")
        return 0
    except KeyboardInterrupt:
        print("Đã dừng streaming job theo yêu cầu.")
        return 130
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
