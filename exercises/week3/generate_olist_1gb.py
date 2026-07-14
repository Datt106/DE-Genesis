"""Tạo bộ dữ liệu order item Olist tổng hợp có kích thước tối thiểu 1 GiB.

Script nhân bản có kiểm soát bảng ``olist_order_items_dataset.csv`` và thêm
``replica_id`` cùng ``synthetic_item_id``. Các khóa Olist gốc vẫn được giữ để
job batch có thể join với orders, customers, products và sellers. Dữ liệu tạo
ra chỉ phục vụ đo hiệu năng; đây không phải dữ liệu giao dịch mới của Olist.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import datetime, timezone
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_INPUT = "/workspace/data/olist/olist_order_items_dataset.csv"
DEFAULT_OUTPUT = "hdfs://namenode:9000/data/week3/raw/order_items_1gb_csv"
GIB = 1024**3

SOURCE_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

SOURCE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DecimalType(18, 2), True),
        StructField("freight_value", DecimalType(18, 2), True),
    ]
)


def configure_utf8_console() -> None:
    """Giữ thông báo tiếng Việt đọc được trên PowerShell."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tạo CSV Olist tổng hợp có kích thước thực tế tối thiểu 1 GiB."
    )
    parser.add_argument(
        "--input",
        default=os.getenv("WEEK3_OLIST_ITEMS", DEFAULT_INPUT),
        help="CSV order items Olist gốc (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("WEEK3_LARGE_INPUT", DEFAULT_OUTPUT),
        help="Thư mục CSV đầu ra local hoặc HDFS (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--target-gib",
        type=float,
        default=float(os.getenv("WEEK3_TARGET_GIB", "1.0")),
        help="Kích thước tối thiểu theo GiB, 1 GiB = 1024^3 byte.",
    )
    parser.add_argument(
        "--copies",
        type=int,
        default=None,
        help="Số bản sao. Bỏ trống để Spark ước lượng theo kích thước mục tiêu.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=int(os.getenv("WEEK3_GENERATOR_PARTITIONS", "16")),
        help="Số part file CSV đầu ra (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--mode",
        choices=("overwrite", "errorifexists"),
        default="overwrite",
        help="Chế độ ghi dữ liệu (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Spark master khi chạy trực tiếp bằng Python.",
    )
    args = parser.parse_args(argv)

    if args.target_gib <= 0:
        parser.error("--target-gib phải lớn hơn 0")
    if args.copies is not None and args.copies < 1:
        parser.error("--copies phải lớn hơn hoặc bằng 1")
    if args.partitions < 1:
        parser.error("--partitions phải lớn hơn hoặc bằng 1")
    return args


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("de-genesis-week3-generate-olist-1gb")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
    )
    hdfs_replication = os.getenv("WEEK3_HDFS_REPLICATION")
    if hdfs_replication:
        builder = builder.config("spark.hadoop.dfs.replication", hdfs_replication)
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_header(spark: SparkSession, path: str) -> None:
    available = spark.read.option("header", "true").csv(path).columns
    missing = sorted(set(SOURCE_COLUMNS) - set(available))
    if missing:
        raise ValueError(f"CSV order items thiếu cột: {', '.join(missing)}")


def read_source(spark: SparkSession, path: str) -> DataFrame:
    validate_header(spark, path)
    source = spark.read.option("header", "true").schema(SOURCE_SCHEMA).csv(path)
    invalid = source.filter(
        F.col("order_id").isNull()
        | F.col("order_item_id").isNull()
        | F.col("product_id").isNull()
        | F.col("seller_id").isNull()
        | F.col("price").isNull()
        | (F.col("price") < 0)
        | F.col("freight_value").isNull()
        | (F.col("freight_value") < 0)
    ).count()
    if invalid:
        raise ValueError(f"CSV order items có {invalid:,} dòng không hợp lệ")
    return source


def estimate_replica_count(
    *,
    base_rows: int,
    average_source_row_bytes: float,
    target_bytes: int,
    safety_factor: float = 1.08,
) -> int:
    """Ước lượng số bản sao và chừa biên để đầu ra vượt kích thước mục tiêu."""

    if base_rows <= 0 or average_source_row_bytes <= 0 or target_bytes <= 0:
        raise ValueError("Các tham số ước lượng phải lớn hơn 0")

    # Hai cột mới thêm replica_id, synthetic_item_id (chứa order_id 32 ký tự)
    # và dấu phân cách làm mỗi dòng dài thêm khoảng 40-42 byte.
    estimated_output_row_bytes = average_source_row_bytes + 42.0
    copies = math.ceil(
        target_bytes * safety_factor / (base_rows * estimated_output_row_bytes)
    )
    return max(1, copies)


def data_size_bytes(spark: SparkSession, path: str) -> int:
    """Tính tổng byte của data file qua Hadoop FileSystem, hỗ trợ local/HDFS."""

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    status = fs.getFileStatus(jpath)
    if status.isFile():
        return int(status.getLen())

    total = 0
    files = fs.listFiles(jpath, True)
    while files.hasNext():
        file_status = files.next()
        name = file_status.getPath().getName()
        if not name.startswith(("_", ".")):
            total += int(file_status.getLen())
    return total


def average_csv_row_bytes(source: DataFrame) -> float:
    serialized = F.concat_ws(
        ",",
        *[F.coalesce(F.col(column).cast("string"), F.lit("")) for column in SOURCE_COLUMNS],
    )
    value = source.select(F.avg(F.length(serialized) + F.lit(1))).first()[0]
    if value is None:
        raise ValueError("CSV order items không có dữ liệu")
    return float(value)


def build_expanded(source: DataFrame, copies: int) -> DataFrame:
    spark = source.sparkSession
    replicas = spark.range(copies).select(F.col("id").cast("int").alias("replica_id"))
    return (
        source.crossJoin(F.broadcast(replicas))
        .withColumn(
            "synthetic_item_id",
            F.concat_ws(
                ":",
                F.col("replica_id").cast("string"),
                F.col("order_id"),
                F.col("order_item_id").cast("string"),
            ),
        )
        .select(
            "replica_id",
            "synthetic_item_id",
            *SOURCE_COLUMNS,
        )
    )


def report_path(output_path: str) -> str:
    return f"{output_path.rstrip('/')}_generation_report"


def run(args: argparse.Namespace) -> dict[str, object]:
    spark = create_spark_session(args)
    try:
        source = read_source(spark, args.input).cache()
        base_rows = source.count()
        if base_rows == 0:
            raise ValueError("CSV order items không có dòng dữ liệu")

        average_bytes = average_csv_row_bytes(source)
        target_bytes = math.ceil(args.target_gib * GIB)
        copies = args.copies or estimate_replica_count(
            base_rows=base_rows,
            average_source_row_bytes=average_bytes,
            target_bytes=target_bytes,
        )
        output_rows = base_rows * copies

        print(
            "Chuẩn bị tạo dữ liệu: "
            f"{base_rows:,} dòng gốc x {copies:,} bản sao = {output_rows:,} dòng."
        )
        expanded = build_expanded(source, copies).repartition(
            args.partitions, "replica_id"
        )
        (
            expanded.write.mode(args.mode)
            .option("header", "true")
            .option("emptyValue", "")
            .csv(args.output)
        )

        actual_bytes = data_size_bytes(spark, args.output)
        meets_target = actual_bytes >= target_bytes
        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input_path": args.input,
            "output_path": args.output,
            "base_rows": int(base_rows),
            "replica_count": int(copies),
            "output_rows": int(output_rows),
            "target_bytes": int(target_bytes),
            "actual_bytes": int(actual_bytes),
            "actual_gib": round(actual_bytes / GIB, 4),
            "partitions": int(args.partitions),
            "meets_target": bool(meets_target),
            "is_synthetic_expansion": True,
        }
        spark.createDataFrame([report]).coalesce(1).write.mode("overwrite").json(
            report_path(args.output)
        )

        if not meets_target:
            suggested = math.ceil(copies * target_bytes / max(actual_bytes, 1) * 1.05)
            raise RuntimeError(
                "Dữ liệu chưa đạt kích thước mục tiêu: "
                f"{actual_bytes / GIB:.3f} GiB < {args.target_gib:.3f} GiB. "
                f"Chạy lại với --copies {suggested}."
            )

        print(
            f"Đã tạo {output_rows:,} dòng, {actual_bytes:,} byte "
            f"({actual_bytes / GIB:.3f} GiB)."
        )
        print(f"Dữ liệu: {args.output}")
        print(f"Báo cáo: {report_path(args.output)}")
        return report
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> None:
    configure_utf8_console()
    run(parse_args(argv))


if __name__ == "__main__":
    main()
