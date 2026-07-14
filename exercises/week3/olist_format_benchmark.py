"""Pipeline Spark batch và benchmark CSV/Parquet/ORC trên Olist mở rộng.

Luồng xử lý dùng DataFrame cho các phép join/biến đổi, Spark SQL và RDD cho
ba cách tổng hợp tương đương, sau đó ghi cùng một DataFrame đã chuẩn hóa ra
CSV, Parquet và ORC. Benchmark chỉ đo khi một action Spark thực sự hoàn tất.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Iterator, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_ITEMS = "hdfs://namenode:9000/data/week3/raw/order_items_1gb_csv"
DEFAULT_OLIST_ROOT = "/workspace/data/olist"
DEFAULT_OUTPUT = "hdfs://namenode:9000/data/week3/benchmark"

FORMAT_ORDER = ("csv", "parquet", "orc")
WORKLOAD_ORDER = ("full_scan_aggregate", "filter_group_aggregate")
REPORT_DATASETS = (
    "quality_report",
    "format_storage_report",
    "benchmark_trials",
    "benchmark_summary",
    "physical_plans",
    "run_status",
)

LARGE_ITEM_COLUMNS = [
    "replica_id",
    "synthetic_item_id",
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

LARGE_ITEM_SCHEMA = StructType(
    [
        StructField("replica_id", IntegerType(), True),
        StructField("synthetic_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DecimalType(18, 2), True),
        StructField("freight_value", DecimalType(18, 2), True),
    ]
)

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
    ]
)

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]
CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ]
)

PRODUCT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]
PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", IntegerType(), True),
        StructField("product_height_cm", IntegerType(), True),
        StructField("product_width_cm", IntegerType(), True),
    ]
)

SELLER_COLUMNS = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
]
SELLER_SCHEMA = StructType(
    [
        StructField("seller_id", StringType(), True),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
    ]
)

CATEGORY_COLUMNS = ["product_category_name", "product_category_name_english"]
CATEGORY_SCHEMA = StructType(
    [
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
    ]
)

CURATED_FILE_SCHEMA = StructType(
    [
        StructField("replica_id", IntegerType(), False),
        StructField("synthetic_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("purchase_timestamp", TimestampType(), False),
        StructField("purchase_date", DateType(), False),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("customer_city", StringType(), False),
        StructField("customer_state", StringType(), False),
        StructField("seller_city", StringType(), False),
        StructField("seller_state", StringType(), False),
        StructField("category_portuguese", StringType(), False),
        StructField("category_english", StringType(), False),
        StructField("price", DecimalType(18, 2), False),
        StructField("freight_value", DecimalType(18, 2), False),
        StructField("gross_amount", DecimalType(20, 2), False),
    ]
)

CATEGORY_SUMMARY_SCHEMA = StructType(
    [
        StructField("category_english", StringType(), False),
        StructField("line_count", LongType(), False),
        StructField("gross_revenue", DecimalType(38, 2), False),
    ]
)


def configure_utf8_console() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")


def join_spark_path(root: str, child: str) -> str:
    return f"{root.rstrip('/')}/{child.lstrip('/')}"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join Olist 1 GiB+ và benchmark CSV, Parquet, ORC bằng Spark."
    )
    parser.add_argument(
        "--items",
        default=os.getenv("WEEK3_LARGE_INPUT", DEFAULT_ITEMS),
        help="Thư mục CSV order items đã mở rộng (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--olist-root",
        default=os.getenv("OLIST_DATA_DIR", DEFAULT_OLIST_ROOT),
        help="Thư mục chứa các CSV Olist gốc (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("WEEK3_BENCHMARK_OUTPUT", DEFAULT_OUTPUT),
        help="Thư mục kết quả local hoặc HDFS (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--mode",
        choices=("overwrite", "errorifexists"),
        default="overwrite",
        help="Chế độ ghi từng dataset con (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(os.getenv("WEEK3_SHUFFLE_PARTITIONS", "16")),
        help="Số partition shuffle (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=int(os.getenv("WEEK3_OUTPUT_PARTITIONS", "16")),
        help="Số partition trước khi ghi ba định dạng (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=int(os.getenv("WEEK3_BENCHMARK_WARMUPS", "1")),
        help="Số lượt làm nóng cho mỗi format/workload, không tính vào kết quả.",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=int(os.getenv("WEEK3_BENCHMARK_TRIALS", "3")),
        help="Số lần đo cho mỗi format/workload (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed xáo trộn thứ tự benchmark (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--allow-unmatched",
        action="store_true",
        help="Không dừng nếu fact không tìm thấy dimension; giá trị sẽ là UNKNOWN.",
    )
    parser.add_argument(
        "--allow-small-input",
        action="store_true",
        help="Cho phép input dưới 1 GiB, chỉ dùng cho smoke test phát triển.",
    )
    parser.add_argument("--master", default=None, help="Spark master khi chạy trực tiếp.")
    args = parser.parse_args(argv)
    for name in ("shuffle_partitions", "output_partitions", "trials"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} phải lớn hơn hoặc bằng 1")
    if args.warmups < 0:
        parser.error("--warmups phải lớn hơn hoặc bằng 0")
    return args


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName("de-genesis-week3-olist-format-benchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
    )
    hdfs_replication = os.getenv("WEEK3_HDFS_REPLICATION")
    if hdfs_replication:
        builder = builder.config("spark.hadoop.dfs.replication", hdfs_replication)
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_header(
    spark: SparkSession, path: str, required_columns: Iterable[str], dataset: str
) -> None:
    available = spark.read.option("header", "true").csv(path).columns
    missing = sorted(set(required_columns) - set(available))
    duplicated = sorted({name for name in available if available.count(name) > 1})
    if missing or duplicated:
        details = []
        if missing:
            details.append("thiếu " + ", ".join(missing))
        if duplicated:
            details.append("trùng " + ", ".join(duplicated))
        raise ValueError(f"Header {dataset} không hợp lệ: {'; '.join(details)}")


def read_csv(
    spark: SparkSession,
    path: str,
    columns: Sequence[str],
    schema: StructType,
    dataset: str,
) -> DataFrame:
    validate_header(spark, path, columns, dataset)
    return spark.read.option("header", "true").schema(schema).csv(path)


def read_inputs(
    spark: SparkSession, items_path: str, olist_root: str
) -> dict[str, DataFrame]:
    return {
        "items": read_csv(
            spark, items_path, LARGE_ITEM_COLUMNS, LARGE_ITEM_SCHEMA, "order_items_1gb"
        ),
        "orders": read_csv(
            spark,
            join_spark_path(olist_root, "olist_orders_dataset.csv"),
            ORDER_COLUMNS,
            ORDER_SCHEMA,
            "orders",
        ),
        "customers": read_csv(
            spark,
            join_spark_path(olist_root, "olist_customers_dataset.csv"),
            CUSTOMER_COLUMNS,
            CUSTOMER_SCHEMA,
            "customers",
        ),
        "products": read_csv(
            spark,
            join_spark_path(olist_root, "olist_products_dataset.csv"),
            PRODUCT_COLUMNS,
            PRODUCT_SCHEMA,
            "products",
        ),
        "sellers": read_csv(
            spark,
            join_spark_path(olist_root, "olist_sellers_dataset.csv"),
            SELLER_COLUMNS,
            SELLER_SCHEMA,
            "sellers",
        ),
        "categories": read_csv(
            spark,
            join_spark_path(olist_root, "product_category_name_translation.csv"),
            CATEGORY_COLUMNS,
            CATEGORY_SCHEMA,
            "category_translation",
        ),
    }


def validate_unique_key(dataframe: DataFrame, key: str, dataset: str) -> None:
    duplicate = (
        dataframe.groupBy(key).count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicate:
        raise ValueError(f"Dimension {dataset} bị trùng khóa {key}")


def build_joined(inputs: dict[str, DataFrame]) -> DataFrame:
    items = inputs["items"].alias("i")
    orders = inputs["orders"].select(
        "order_id",
        "customer_id",
        "order_status",
        F.col("order_purchase_timestamp").alias("purchase_timestamp"),
        F.lit(1).alias("_order_found"),
    )
    customers = inputs["customers"].select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        F.lit(1).alias("_customer_found"),
    )
    products = inputs["products"].select(
        "product_id",
        "product_category_name",
        F.lit(1).alias("_product_found"),
    )
    sellers = inputs["sellers"].select(
        "seller_id",
        "seller_city",
        "seller_state",
        F.lit(1).alias("_seller_found"),
    )
    categories = inputs["categories"].select(
        "product_category_name",
        "product_category_name_english",
    )

    # Các dimension đều nhỏ hơn fact 1 GiB+, broadcast tránh shuffle fact nhiều lần.
    return (
        items.join(F.broadcast(orders), "order_id", "left")
        .join(F.broadcast(customers), "customer_id", "left")
        .join(F.broadcast(products), "product_id", "left")
        .join(F.broadcast(sellers), "seller_id", "left")
        .join(F.broadcast(categories), "product_category_name", "left")
    )


def collect_join_quality(joined: DataFrame) -> dict[str, int]:
    row = joined.agg(
        F.count("*").alias("source_rows"),
        F.sum(F.when(F.col("_order_found").isNull(), 1).otherwise(0)).alias(
            "unmatched_orders"
        ),
        F.sum(F.when(F.col("_customer_found").isNull(), 1).otherwise(0)).alias(
            "unmatched_customers"
        ),
        F.sum(F.when(F.col("_product_found").isNull(), 1).otherwise(0)).alias(
            "unmatched_products"
        ),
        F.sum(F.when(F.col("_seller_found").isNull(), 1).otherwise(0)).alias(
            "unmatched_sellers"
        ),
    ).first()
    return {name: int(row[name] or 0) for name in row.asDict()}


def build_curated(joined: DataFrame) -> DataFrame:
    unknown = F.lit("UNKNOWN")
    return joined.select(
        F.col("replica_id").cast("int").alias("replica_id"),
        F.col("synthetic_item_id"),
        F.col("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.coalesce(F.col("customer_id"), unknown).alias("customer_id"),
        F.coalesce(F.col("customer_unique_id"), unknown).alias("customer_unique_id"),
        F.col("product_id"),
        F.col("seller_id"),
        F.coalesce(F.col("order_status"), unknown).alias("order_status"),
        F.col("purchase_timestamp"),
        F.to_date("purchase_timestamp").alias("purchase_date"),
        F.col("shipping_limit_date"),
        F.coalesce(F.col("customer_city"), unknown).alias("customer_city"),
        F.coalesce(F.col("customer_state"), unknown).alias("customer_state"),
        F.coalesce(F.col("seller_city"), unknown).alias("seller_city"),
        F.coalesce(F.col("seller_state"), unknown).alias("seller_state"),
        F.coalesce(F.col("product_category_name"), unknown).alias(
            "category_portuguese"
        ),
        F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name"),
            unknown,
        ).alias("category_english"),
        F.col("price").cast(DecimalType(18, 2)).alias("price"),
        F.col("freight_value").cast(DecimalType(18, 2)).alias("freight_value"),
        (F.col("price") + F.col("freight_value"))
        .cast(DecimalType(20, 2))
        .alias("gross_amount"),
        F.year("purchase_timestamp").alias("purchase_year"),
        F.month("purchase_timestamp").alias("purchase_month"),
    )


def build_dataframe_category_summary(curated: DataFrame) -> DataFrame:
    return (
        curated.groupBy("category_english")
        .agg(
            F.count("*").cast("long").alias("line_count"),
            F.sum("gross_amount").cast(DecimalType(38, 2)).alias("gross_revenue"),
        )
        .orderBy(F.desc("gross_revenue"), "category_english")
    )


def build_sql_category_summary(curated: DataFrame) -> DataFrame:
    curated.createOrReplaceTempView("week3_olist_sales")
    return curated.sparkSession.sql(
        """
        SELECT
            category_english,
            CAST(COUNT(*) AS BIGINT) AS line_count,
            CAST(SUM(gross_amount) AS DECIMAL(38, 2)) AS gross_revenue
        FROM week3_olist_sales
        GROUP BY category_english
        ORDER BY gross_revenue DESC, category_english
        """
    )


def _aggregate_category_partition(
    rows: Iterator[Row],
) -> Iterator[tuple[str, tuple[int, Decimal]]]:
    totals: dict[str, list[object]] = defaultdict(lambda: [0, Decimal("0.00")])
    for row in rows:
        category = str(row["category_english"])
        totals[category][0] = int(totals[category][0]) + 1
        totals[category][1] = Decimal(totals[category][1]) + Decimal(
            row["gross_amount"]
        )
    for category, (count, revenue) in totals.items():
        yield category, (int(count), Decimal(revenue))


def build_rdd_category_summary(curated: DataFrame) -> DataFrame:
    # PySpark worker cần import được module chứa hàm mapPartitions. spark-submit
    # tự phân phối file chính; addPyFile giữ cùng hành vi khi hàm được gọi từ pytest.
    curated.sparkSession.sparkContext.addPyFile(os.path.abspath(__file__))
    pairs = (
        curated.select("category_english", "gross_amount")
        .rdd.mapPartitions(_aggregate_category_partition)
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
        .map(lambda item: (item[0], int(item[1][0]), Decimal(item[1][1])))
    )
    return curated.sparkSession.createDataFrame(pairs, CATEGORY_SUMMARY_SCHEMA).orderBy(
        F.desc("gross_revenue"), "category_english"
    )


def summary_signature(dataframe: DataFrame) -> list[tuple[str, int, str]]:
    rows = dataframe.select(
        "category_english", "line_count", "gross_revenue"
    ).collect()
    return sorted(
        (
            str(row["category_english"]),
            int(row["line_count"]),
            format(Decimal(row["gross_revenue"]), ".2f"),
        )
        for row in rows
    )


def assert_api_equivalence(
    dataframe_summary: DataFrame,
    sql_summary: DataFrame,
    rdd_summary: DataFrame,
) -> dict[str, bool]:
    dataframe_signature = summary_signature(dataframe_summary)
    sql_signature = summary_signature(sql_summary)
    rdd_signature = summary_signature(rdd_summary)
    dataframe_sql_equal = dataframe_signature == sql_signature
    dataframe_rdd_equal = dataframe_signature == rdd_signature
    if not dataframe_sql_equal or not dataframe_rdd_equal:
        raise RuntimeError("Kết quả tổng hợp DataFrame, Spark SQL và RDD không khớp")
    return {
        "dataframe_sql_equal": dataframe_sql_equal,
        "dataframe_rdd_equal": dataframe_rdd_equal,
    }


def build_other_aggregates(curated: DataFrame) -> dict[str, DataFrame]:
    common = [
        F.count("*").cast("long").alias("line_count"),
        F.countDistinct("synthetic_item_id").alias("distinct_items"),
        F.sum("price").cast(DecimalType(38, 2)).alias("item_revenue"),
        F.sum("freight_value").cast(DecimalType(38, 2)).alias("freight_revenue"),
        F.sum("gross_amount").cast(DecimalType(38, 2)).alias("gross_revenue"),
    ]
    return {
        "state_summary": curated.groupBy("customer_state")
        .agg(*common)
        .orderBy(F.desc("gross_revenue"), "customer_state"),
        "monthly_summary": curated.groupBy("purchase_year", "purchase_month")
        .agg(*common)
        .orderBy("purchase_year", "purchase_month"),
    }


def data_file_metrics(spark: SparkSession, path: str) -> dict[str, int]:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    status = fs.getFileStatus(jpath)

    sizes: list[int] = []
    if status.isFile():
        sizes.append(int(status.getLen()))
    else:
        files = fs.listFiles(jpath, True)
        while files.hasNext():
            file_status = files.next()
            name = file_status.getPath().getName()
            if not name.startswith(("_", ".")):
                sizes.append(int(file_status.getLen()))

    return {
        "data_bytes": int(sum(sizes)),
        "data_file_count": int(len(sizes)),
        "min_file_bytes": int(min(sizes, default=0)),
        "max_file_bytes": int(max(sizes, default=0)),
    }


def delete_spark_path_if_exists(spark: SparkSession, path: str) -> None:
    """Xóa đúng một path báo cáo cũ qua Hadoop FileSystem."""

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(hadoop_conf)
    if fs.exists(jpath) and not fs.delete(jpath, True):
        raise RuntimeError(f"Không thể xóa báo cáo cũ: {path}")


def clear_previous_reports(spark: SparkSession, output: str) -> None:
    """Ngăn report/_SUCCESS cũ bị hiểu nhầm là kết quả của lần chạy mới."""

    for child in REPORT_DATASETS:
        delete_spark_path_if_exists(spark, join_spark_path(output, child))


def format_paths(output: str) -> dict[str, str]:
    return {
        "csv": join_spark_path(output, "curated_csv"),
        "parquet": join_spark_path(output, "curated_parquet"),
        "orc": join_spark_path(output, "curated_orc"),
    }


def write_formats(
    curated: DataFrame, output: str, mode: str
) -> tuple[dict[str, str], dict[str, float]]:
    paths = format_paths(output)
    durations: dict[str, float] = {}

    for data_format in FORMAT_ORDER:
        started = time.perf_counter()
        writer = curated.write.mode(mode).partitionBy("purchase_year", "purchase_month")
        if data_format == "csv":
            writer.option("header", "true").option("emptyValue", "").csv(
                paths[data_format]
            )
        elif data_format == "parquet":
            writer.option("compression", "snappy").parquet(paths[data_format])
        else:
            writer.option("compression", "snappy").orc(paths[data_format])
        durations[data_format] = round(time.perf_counter() - started, 6)
    return paths, durations


def read_format(spark: SparkSession, data_format: str, path: str) -> DataFrame:
    if data_format == "csv":
        return spark.read.option("header", "true").schema(CURATED_FILE_SCHEMA).csv(path)
    if data_format == "parquet":
        return spark.read.parquet(path)
    if data_format == "orc":
        return spark.read.orc(path)
    raise ValueError(f"Định dạng không hỗ trợ: {data_format}")


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def execute_workload(
    dataframe: DataFrame, workload: str, benchmark_year: int
) -> tuple[int, str, str, str]:
    if workload == "full_scan_aggregate":
        query = dataframe.agg(
            F.count("*").alias("row_count"),
            F.sum("price").cast(DecimalType(38, 2)).alias("price_total"),
            F.sum("freight_value").cast(DecimalType(38, 2)).alias("freight_total"),
            F.sum("gross_amount").cast(DecimalType(38, 2)).alias("gross_total"),
        )
        row = query.first()
        result = {
            "row_count": int(row["row_count"]),
            "price_total": format(Decimal(row["price_total"]), ".2f"),
            "freight_total": format(Decimal(row["freight_total"]), ".2f"),
            "gross_total": format(Decimal(row["gross_total"]), ".2f"),
        }
        result_rows = 1
    elif workload == "filter_group_aggregate":
        query = (
            dataframe.filter(F.col("purchase_year") == benchmark_year)
            .groupBy("customer_state", "category_english")
            .agg(
                F.count("*").alias("line_count"),
                F.sum("gross_amount")
                .cast(DecimalType(38, 2))
                .alias("gross_revenue"),
            )
            .orderBy("customer_state", "category_english")
        )
        rows = query.collect()
        result = [
            {
                "customer_state": str(row["customer_state"]),
                "category_english": str(row["category_english"]),
                "line_count": int(row["line_count"]),
                "gross_revenue": format(Decimal(row["gross_revenue"]), ".2f"),
            }
            for row in rows
        ]
        result_rows = len(rows)
    else:
        raise ValueError(f"Workload không hỗ trợ: {workload}")

    serialized = _canonical_json(result)
    checksum = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    physical_plan = query._jdf.queryExecution().executedPlan().toString()
    return result_rows, checksum, serialized, physical_plan


def run_benchmark(
    spark: SparkSession,
    paths: dict[str, str],
    *,
    benchmark_year: int,
    warmups: int,
    trials: int,
    seed: int,
) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    records: list[dict[str, object]] = []
    plans: dict[tuple[str, str], str] = {}
    tasks = [(fmt, workload) for fmt in FORMAT_ORDER for workload in WORKLOAD_ORDER]

    cycles = [(True, index + 1) for index in range(warmups)] + [
        (False, index + 1) for index in range(trials)
    ]
    for cycle_index, (is_warmup, trial_number) in enumerate(cycles):
        ordered_tasks = list(tasks)
        random.Random(seed + cycle_index).shuffle(ordered_tasks)
        for data_format, workload in ordered_tasks:
            spark.catalog.clearCache()
            started = time.perf_counter()
            dataframe = read_format(spark, data_format, paths[data_format])
            result_rows, checksum, _, physical_plan = execute_workload(
                dataframe, workload, benchmark_year
            )
            duration = time.perf_counter() - started
            if (data_format, workload) not in plans:
                plans[(data_format, workload)] = physical_plan
            records.append(
                {
                    "format": data_format,
                    "compression": "none" if data_format == "csv" else "snappy",
                    "workload": workload,
                    "trial": int(trial_number),
                    "is_warmup": bool(is_warmup),
                    "duration_seconds": round(duration, 6),
                    "result_rows": int(result_rows),
                    "result_checksum": checksum,
                }
            )

    plan_rows = [
        {"format": fmt, "workload": workload, "physical_plan": plan}
        for (fmt, workload), plan in sorted(plans.items())
    ]
    return records, plan_rows


def validate_benchmark(records: list[dict[str, object]]) -> bool:
    for workload in WORKLOAD_ORDER:
        checksums = {
            str(record["result_checksum"])
            for record in records
            if record["workload"] == workload
        }
        if len(checksums) != 1:
            raise RuntimeError(
                f"Checksum benchmark không đồng nhất giữa các format: {workload}"
            )
    return True


def summarize_benchmark(
    records: list[dict[str, object]],
    format_metrics: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    csv_bytes = int(format_metrics["csv"]["data_bytes"])
    for data_format in FORMAT_ORDER:
        for workload in WORKLOAD_ORDER:
            values = [
                float(record["duration_seconds"])
                for record in records
                if record["format"] == data_format
                and record["workload"] == workload
                and not bool(record["is_warmup"])
            ]
            metric = format_metrics[data_format]
            summaries.append(
                {
                    "format": data_format,
                    "compression": metric["compression"],
                    "workload": workload,
                    "trials": len(values),
                    "median_seconds": round(statistics.median(values), 6),
                    "min_seconds": round(min(values), 6),
                    "max_seconds": round(max(values), 6),
                    "data_bytes": int(metric["data_bytes"]),
                    "data_file_count": int(metric["data_file_count"]),
                    "size_ratio_vs_csv": round(
                        int(metric["data_bytes"]) / max(csv_bytes, 1), 6
                    ),
                    "write_seconds": float(metric["write_seconds"]),
                }
            )
    return summaries


def write_reports(
    spark: SparkSession,
    output: str,
    mode: str,
    *,
    quality: dict[str, object],
    format_metrics: dict[str, dict[str, object]],
    trials: list[dict[str, object]],
    summaries: list[dict[str, object]],
    plans: list[dict[str, str]],
) -> None:
    spark.createDataFrame([quality]).coalesce(1).write.mode(mode).json(
        join_spark_path(output, "quality_report")
    )
    spark.createDataFrame(list(format_metrics.values())).coalesce(1).write.mode(
        mode
    ).json(join_spark_path(output, "format_storage_report"))
    spark.createDataFrame(trials).coalesce(1).write.mode(mode).json(
        join_spark_path(output, "benchmark_trials")
    )
    spark.createDataFrame(summaries).coalesce(1).write.mode(mode).option(
        "header", "true"
    ).csv(join_spark_path(output, "benchmark_summary"))
    spark.createDataFrame(plans).coalesce(1).write.mode(mode).json(
        join_spark_path(output, "physical_plans")
    )
    # Ghi cuối cùng: chỉ marker này xác nhận toàn bộ data, benchmark và report
    # của lần chạy đã hoàn tất. Nếu job lỗi giữa chừng thì run_status không có.
    spark.createDataFrame(
        [
            {
                "status": "success",
                "generated_at_utc": quality["generated_at_utc"],
                "application_id": quality["application_id"],
                "measured_trials": quality["measured_trials"],
            }
        ]
    ).coalesce(1).write.mode(mode).json(join_spark_path(output, "run_status"))


def run(args: argparse.Namespace) -> dict[str, object]:
    spark = create_spark_session(args)
    try:
        input_metrics = data_file_metrics(spark, args.items)
        if input_metrics["data_bytes"] < 1024**3 and not args.allow_small_input:
            raise RuntimeError(
                "Input chưa đạt 1 GiB: "
                f"{input_metrics['data_bytes']:,} byte < {1024**3:,} byte. "
                "Hãy chạy generate_olist_1gb.py trước; --allow-small-input chỉ dành "
                "cho smoke test."
            )
        if args.mode == "overwrite":
            clear_previous_reports(spark, args.output)
        inputs = read_inputs(spark, args.items, args.olist_root)
        for name, key in (
            ("orders", "order_id"),
            ("customers", "customer_id"),
            ("products", "product_id"),
            ("sellers", "seller_id"),
            ("categories", "product_category_name"),
        ):
            validate_unique_key(inputs[name], key, name)

        joined = build_joined(inputs).persist(StorageLevel.DISK_ONLY)
        join_quality = collect_join_quality(joined)
        unmatched_total = sum(
            join_quality[name]
            for name in (
                "unmatched_orders",
                "unmatched_customers",
                "unmatched_products",
                "unmatched_sellers",
            )
        )
        if unmatched_total and not args.allow_unmatched:
            raise RuntimeError(
                "Join dimension có khóa không khớp: "
                + ", ".join(
                    f"{name}={join_quality[name]:,}"
                    for name in (
                        "unmatched_orders",
                        "unmatched_customers",
                        "unmatched_products",
                        "unmatched_sellers",
                    )
                )
            )

        curated = (
            build_curated(joined)
            .repartition(
                args.output_partitions, "purchase_year", "purchase_month"
            )
            .persist(StorageLevel.DISK_ONLY)
        )
        curated_stats = curated.agg(
            F.count("*").alias("curated_rows"),
            F.min("purchase_year").alias("min_year"),
            F.max("purchase_year").alias("max_year"),
            F.sum("gross_amount").cast(DecimalType(38, 2)).alias("gross_revenue"),
        ).first()
        joined.unpersist()

        curated_rows = int(curated_stats["curated_rows"])
        if curated_rows != join_quality["source_rows"]:
            raise RuntimeError("Số dòng curated khác số dòng fact đầu vào")
        benchmark_year = int(curated_stats["max_year"])

        dataframe_summary = build_dataframe_category_summary(curated).persist()
        sql_summary = build_sql_category_summary(curated)
        rdd_summary = build_rdd_category_summary(curated).persist()
        api_validation = assert_api_equivalence(
            dataframe_summary, sql_summary, rdd_summary
        )
        other_aggregates = build_other_aggregates(curated)

        paths, write_durations = write_formats(curated, args.output, args.mode)
        dataframe_summary.write.mode(args.mode).parquet(
            join_spark_path(args.output, "category_summary")
        )
        rdd_summary.write.mode(args.mode).parquet(
            join_spark_path(args.output, "rdd_category_summary")
        )
        for name, dataframe in other_aggregates.items():
            dataframe.write.mode(args.mode).parquet(join_spark_path(args.output, name))

        format_metrics: dict[str, dict[str, object]] = {}
        for data_format in FORMAT_ORDER:
            metric = data_file_metrics(spark, paths[data_format])
            metric.update(
                {
                    "format": data_format,
                    "compression": "none" if data_format == "csv" else "snappy",
                    "path": paths[data_format],
                    "write_seconds": write_durations[data_format],
                }
            )
            format_metrics[data_format] = metric

        dataframe_summary.unpersist()
        rdd_summary.unpersist()
        curated.unpersist()
        spark.catalog.clearCache()

        benchmark_trials, plans = run_benchmark(
            spark,
            paths,
            benchmark_year=benchmark_year,
            warmups=args.warmups,
            trials=args.trials,
            seed=args.seed,
        )
        benchmark_equal = validate_benchmark(benchmark_trials)
        benchmark_summaries = summarize_benchmark(benchmark_trials, format_metrics)

        quality: dict[str, object] = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "application_id": spark.sparkContext.applicationId,
            "input_path": args.items,
            "output_path": args.output,
            "input_data_bytes": int(input_metrics["data_bytes"]),
            "input_is_at_least_1_gib": bool(input_metrics["data_bytes"] >= 1024**3),
            "source_rows": int(join_quality["source_rows"]),
            "curated_rows": curated_rows,
            "gross_revenue": format(Decimal(curated_stats["gross_revenue"]), ".2f"),
            "min_year": int(curated_stats["min_year"]),
            "max_year": int(curated_stats["max_year"]),
            "benchmark_year": benchmark_year,
            "unmatched_orders": int(join_quality["unmatched_orders"]),
            "unmatched_customers": int(join_quality["unmatched_customers"]),
            "unmatched_products": int(join_quality["unmatched_products"]),
            "unmatched_sellers": int(join_quality["unmatched_sellers"]),
            "dataframe_sql_equal": bool(api_validation["dataframe_sql_equal"]),
            "dataframe_rdd_equal": bool(api_validation["dataframe_rdd_equal"]),
            "format_checksums_equal": bool(benchmark_equal),
            "shuffle_partitions": int(args.shuffle_partitions),
            "output_partitions": int(args.output_partitions),
            "warmups_per_case": int(args.warmups),
            "trials_per_case": int(args.trials),
            "measured_trials": int(
                sum(not bool(record["is_warmup"]) for record in benchmark_trials)
            ),
            "benchmark_warning": (
                "Thời gian có thể chịu ảnh hưởng bởi cache của hệ điều hành/Docker; "
                "không diễn giải là cold-disk benchmark."
            ),
        }
        write_reports(
            spark,
            args.output,
            args.mode,
            quality=quality,
            format_metrics=format_metrics,
            trials=benchmark_trials,
            summaries=benchmark_summaries,
            plans=plans,
        )

        print("Hoàn thành pipeline Olist và benchmark Tuần 3.")
        print(
            f"Input={quality['input_data_bytes']:,} byte; "
            f"dòng={curated_rows:,}; doanh thu tổng hợp={quality['gross_revenue']}."
        )
        print(
            "Đối chiếu API: DataFrame=SQL="
            f"{quality['dataframe_sql_equal']}, DataFrame=RDD="
            f"{quality['dataframe_rdd_equal']}."
        )
        for metric in benchmark_summaries:
            print(
                f"{str(metric['format']).upper():7} | {metric['workload']:<23} | "
                f"median={metric['median_seconds']:.3f}s | "
                f"size={int(metric['data_bytes']):,} byte"
            )
        print(f"Kết quả: {args.output}")
        return quality
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> None:
    configure_utf8_console()
    run(parse_args(argv))


if __name__ == "__main__":
    main()
