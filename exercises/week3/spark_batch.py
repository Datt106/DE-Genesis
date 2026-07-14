"""Batch pipeline tuần 3: Spark, HDFS, Parquet và ORC.

Job đọc dữ liệu bán hàng CSV, làm sạch/kiểm tra chất lượng, tính các chỉ số
tổng hợp rồi ghi dữ liệu đã chuẩn hóa ở cả Parquet và ORC. Mọi đường dẫn đều
có thể là đường dẫn local (``/workspace/...``) hoặc URI HDFS
(``hdfs://namenode:9000/...``).
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_INPUT = "/workspace/data/sample/sales.csv"
DEFAULT_OUTPUT = "/workspace/output/week3"

SOURCE_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "region",
]

RAW_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in SOURCE_COLUMNS]
    + [StructField("_corrupt_record", StringType(), True)]
)

QUALITY_REPORT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("generated_at_utc", TimestampType(), False),
        StructField("input_path", StringType(), False),
        StructField("output_path", StringType(), False),
        StructField("source_rows", LongType(), False),
        StructField("duplicates_removed", LongType(), False),
        StructField("rejected_rows", LongType(), False),
        StructField("valid_rows", LongType(), False),
        StructField("distinct_orders", LongType(), False),
        StructField("distinct_customers", LongType(), False),
        StructField("total_quantity", LongType(), False),
        StructField("total_revenue", DecimalType(20, 2), False),
        StructField("parquet_rows", LongType(), False),
        StructField("orc_rows", LongType(), False),
    ]
)


def configure_utf8_console() -> None:
    """Giữ thông báo tiếng Việt đọc được trên PowerShell dùng code page cũ."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")


def join_spark_path(root: str, child: str) -> str:
    """Ghép đường dẫn mà không làm hỏng URI HDFS hoặc đường dẫn trong container."""

    return f"{root.rstrip('/')}/{child.lstrip('/')}"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Xử lý batch dữ liệu bán hàng bằng Spark và ghi kết quả Parquet/ORC."
        )
    )
    parser.add_argument(
        "--input",
        default=os.getenv("WEEK3_INPUT", DEFAULT_INPUT),
        help="CSV nguồn local hoặc HDFS (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("WEEK3_OUTPUT", DEFAULT_OUTPUT),
        help="Thư mục kết quả local hoặc HDFS (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--master",
        default=None,
        help=(
            "Spark master khi chạy trực tiếp bằng Python. Khi dùng spark-submit "
            "thì nên truyền --master cho spark-submit."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("overwrite", "errorifexists"),
        default="overwrite",
        help="Chế độ ghi các dataset kết quả (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(os.getenv("WEEK3_SHUFFLE_PARTITIONS", "2")),
        help="Số partition shuffle cho dữ liệu thực hành (mặc định: %(default)s).",
    )
    args = parser.parse_args(argv)
    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions phải lớn hơn hoặc bằng 1")
    return args


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("de-genesis-week3-batch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    )
    hdfs_replication = os.getenv("WEEK3_HDFS_REPLICATION")
    if hdfs_replication:
        builder = builder.config("spark.hadoop.dfs.replication", hdfs_replication)

    # spark-submit đã truyền spark.master qua SparkConf. --master chỉ phục vụ
    # trường hợp chạy file trực tiếp bằng Python để kiểm thử nhanh.
    if args.master:
        builder = builder.master(args.master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_source_columns(spark: SparkSession, input_path: str) -> None:
    """Dừng sớm nếu header CSV thiếu cột bắt buộc hoặc có tên cột trùng."""

    header = spark.read.option("header", "true").csv(input_path)
    available = header.columns
    missing = sorted(set(SOURCE_COLUMNS) - set(available))
    duplicated = sorted({name for name in available if available.count(name) > 1})
    if missing or duplicated:
        details = []
        if missing:
            details.append(f"thiếu cột: {', '.join(missing)}")
        if duplicated:
            details.append(f"trùng tên cột: {', '.join(duplicated)}")
        raise ValueError("Header CSV không hợp lệ (" + "; ".join(details) + ")")


def read_raw_sales(spark: SparkSession, input_path: str) -> DataFrame:
    validate_source_columns(spark, input_path)
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_SCHEMA)
        .csv(input_path)
        .withColumn("_source_file", F.input_file_name())
    )


def normalize_and_validate(raw: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Chuẩn hóa kiểu dữ liệu, loại bản ghi trùng và tách dòng lỗi."""

    without_duplicates = raw.dropDuplicates(SOURCE_COLUMNS).cache()
    source_rows = raw.count()
    distinct_source_rows = without_duplicates.count()
    duplicates_removed = source_rows - distinct_source_rows

    normalized = (
        without_duplicates
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("order_date", F.to_date(F.trim(F.col("order_date")), "yyyy-MM-dd"))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("quantity", F.trim(F.col("quantity")).cast(IntegerType()))
        .withColumn("unit_price", F.trim(F.col("unit_price")).cast(DecimalType(18, 2)))
        .withColumn("region", F.trim(F.col("region")))
    )

    rejection_rules = [
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("CSV sai cấu trúc")),
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), F.lit("order_id rỗng")),
        F.when(F.col("order_date").isNull(), F.lit("order_date sai định dạng yyyy-MM-dd")),
        F.when(
            F.col("customer_id").isNull() | (F.col("customer_id") == ""),
            F.lit("customer_id rỗng"),
        ),
        F.when(
            F.col("product_id").isNull() | (F.col("product_id") == ""),
            F.lit("product_id rỗng"),
        ),
        F.when(F.col("category").isNull() | (F.col("category") == ""), F.lit("category rỗng")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity phải > 0")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("unit_price phải >= 0")),
        F.when(F.col("region").isNull() | (F.col("region") == ""), F.lit("region rỗng")),
    ]

    checked = normalized.withColumn(
        "rejection_reason", F.concat_ws("; ", *rejection_rules)
    ).cache()

    rejected = checked.filter(F.col("rejection_reason") != "").cache()
    valid = (
        checked.filter(F.col("rejection_reason") == "")
        .drop("_corrupt_record", "rejection_reason")
        .withColumn(
            "line_amount",
            (F.col("quantity").cast(DecimalType(20, 2)) * F.col("unit_price")).cast(
                DecimalType(20, 2)
            ),
        )
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .select(
            "order_id",
            "order_date",
            "customer_id",
            "customer_name",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "line_amount",
            "region",
            "order_year",
            "order_month",
            "_source_file",
        )
        .cache()
    )

    without_duplicates.unpersist()
    return valid, rejected, duplicates_removed


def build_aggregates(valid: DataFrame) -> dict[str, DataFrame]:
    common_metrics = [
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("quantity").cast(LongType()).alias("units_sold"),
        F.sum("line_amount").cast(DecimalType(20, 2)).alias("revenue"),
        F.round(F.avg("line_amount"), 2).cast(DecimalType(20, 2)).alias(
            "average_line_amount"
        ),
    ]

    return {
        "category_summary": valid.groupBy("category").agg(*common_metrics).orderBy(
            F.desc("revenue"), "category"
        ),
        "region_summary": valid.groupBy("region").agg(*common_metrics).orderBy(
            F.desc("revenue"), "region"
        ),
        "daily_summary": valid.groupBy("order_date")
        .agg(*common_metrics)
        .orderBy("order_date"),
    }


def write_datasets(
    valid: DataFrame,
    rejected: DataFrame,
    aggregates: dict[str, DataFrame],
    output_path: str,
    mode: str,
) -> tuple[str, str]:
    parquet_path = join_spark_path(output_path, "curated_sales_parquet")
    orc_path = join_spark_path(output_path, "curated_sales_orc")

    (
        valid.write.mode(mode)
        .option("compression", "snappy")
        .partitionBy("order_year", "order_month")
        .parquet(parquet_path)
    )
    (
        valid.write.mode(mode)
        .option("compression", "snappy")
        .partitionBy("order_year", "order_month")
        .orc(orc_path)
    )
    rejected.write.mode(mode).parquet(join_spark_path(output_path, "rejected_rows"))

    for name, dataset in aggregates.items():
        dataset.write.mode(mode).parquet(join_spark_path(output_path, name))

    return parquet_path, orc_path


def write_quality_report(
    spark: SparkSession,
    *,
    input_path: str,
    output_path: str,
    source_rows: int,
    duplicates_removed: int,
    rejected_rows: int,
    valid: DataFrame,
    parquet_rows: int,
    orc_rows: int,
    mode: str,
) -> dict[str, object]:
    statistics = valid.agg(
        F.count("*").alias("valid_rows"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.coalesce(F.sum("quantity"), F.lit(0)).cast(LongType()).alias("total_quantity"),
        F.coalesce(F.sum("line_amount"), F.lit(Decimal("0.00")))
        .cast(DecimalType(20, 2))
        .alias("total_revenue"),
    ).first()

    report = {
        "run_id": str(uuid.uuid4()),
        "generated_at_utc": datetime.now(timezone.utc).replace(tzinfo=None),
        "input_path": input_path,
        "output_path": output_path,
        "source_rows": int(source_rows),
        "duplicates_removed": int(duplicates_removed),
        "rejected_rows": int(rejected_rows),
        "valid_rows": int(statistics["valid_rows"]),
        "distinct_orders": int(statistics["distinct_orders"]),
        "distinct_customers": int(statistics["distinct_customers"]),
        "total_quantity": int(statistics["total_quantity"]),
        "total_revenue": statistics["total_revenue"],
        "parquet_rows": int(parquet_rows),
        "orc_rows": int(orc_rows),
    }

    spark.createDataFrame([report], schema=QUALITY_REPORT_SCHEMA).coalesce(1).write.mode(
        mode
    ).json(join_spark_path(output_path, "quality_report"))
    return report


def run(args: argparse.Namespace) -> dict[str, object]:
    spark = create_spark_session(args)
    try:
        raw = read_raw_sales(spark, args.input).cache()
        source_rows = raw.count()
        valid, rejected, duplicates_removed = normalize_and_validate(raw)
        valid_rows = valid.count()
        rejected_rows = rejected.count()

        if source_rows != duplicates_removed + valid_rows + rejected_rows:
            raise RuntimeError(
                "Đối soát dòng thất bại: source != duplicate + valid + rejected"
            )

        aggregates = build_aggregates(valid)
        parquet_path, orc_path = write_datasets(
            valid, rejected, aggregates, args.output, args.mode
        )

        # Đọc lại hai định dạng để chứng minh file được ghi hợp lệ và không mất dòng.
        parquet_rows = spark.read.parquet(parquet_path).count()
        orc_rows = spark.read.orc(orc_path).count()
        if parquet_rows != valid_rows or orc_rows != valid_rows:
            raise RuntimeError(
                "Đối soát định dạng thất bại: số dòng Parquet/ORC khác dữ liệu hợp lệ"
            )

        report = write_quality_report(
            spark,
            input_path=args.input,
            output_path=args.output,
            source_rows=source_rows,
            duplicates_removed=duplicates_removed,
            rejected_rows=rejected_rows,
            valid=valid,
            parquet_rows=parquet_rows,
            orc_rows=orc_rows,
            mode=args.mode,
        )

        print("Hoàn thành Spark batch tuần 3.")
        print(
            "Đối soát: "
            f"nguồn={report['source_rows']}, "
            f"hợp lệ={report['valid_rows']}, "
            f"loại trùng={report['duplicates_removed']}, "
            f"từ chối={report['rejected_rows']}."
        )
        print(
            f"Tổng số lượng={report['total_quantity']}, "
            f"doanh thu={report['total_revenue']}."
        )
        print(f"Parquet={report['parquet_rows']} dòng; ORC={report['orc_rows']} dòng.")
        print(f"Kết quả: {args.output}")
        return report
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> None:
    configure_utf8_console()
    run(parse_args(argv))


if __name__ == "__main__":
    main()
