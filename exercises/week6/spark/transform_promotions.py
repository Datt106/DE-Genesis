"""Tạo snapshot promotion vào staging bằng Spark JDBC phân tán."""

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
from exercises.week6.repository import ensure_schema, update_counts


DEFAULT_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"
JDBC_PARTITIONS = 4


def jdbc_configuration(database: DatabaseConfig) -> tuple[str, dict[str, str]]:
    url = f"jdbc:postgresql://{database.host}:{database.port}/{database.database}"
    properties = {
        "user": database.user,
        "password": database.password,
        "driver": "org.postgresql.Driver",
        "fetchsize": os.getenv("WEEK6_JDBC_FETCH_SIZE", "10000"),
    }
    return url, properties


def hash_predicates(column: str, partitions: int = JDBC_PARTITIONS) -> list[str]:
    """Chia JDBC read theo hash ổn định, không kéo toàn bộ dữ liệu về driver."""

    return [
        f"MOD(ABS(hashtext({column})::bigint), {partitions}) = {partition}"
        for partition in range(partitions)
    ]


def load_source_dataframes(
    spark: SparkSession,
    database: DatabaseConfig,
) -> tuple[DataFrame, DataFrame]:
    url, properties = jdbc_configuration(database)
    promotions_query = """
        (
            SELECT promotion_id, product_id, discount_type, discount_value,
                   starts_at, ends_at, version
            FROM (
                SELECT DISTINCT ON (promotion_id)
                    payload->>'promotion_id' AS promotion_id,
                    payload->>'product_id' AS product_id,
                    payload->>'discount_type' AS discount_type,
                    (payload->>'discount_value')::numeric AS discount_value,
                    (payload->>'starts_at')::timestamptz AS starts_at,
                    (payload->>'ends_at')::timestamptz AS ends_at,
                    (payload->>'version')::integer AS version,
                    payload->>'status' AS status
                FROM week6_raw.promotions
                WHERE is_valid
                ORDER BY promotion_id, source_updated_at DESC, ingested_at DESC
            ) AS latest
            WHERE status='active'
        ) AS active_promotions
    """
    sales_query = """
        (
            SELECT sales.order_id,
                   sales.item_number,
                   product.product_id,
                   purchase_date.full_date AS purchase_date,
                   sales.item_price,
                   sales.freight_value
            FROM olist_olap.fact_sales AS sales
            JOIN olist_olap.dim_product AS product
              ON product.product_key = sales.product_key
            LEFT JOIN olist_olap.dim_date AS purchase_date
              ON purchase_date.date_key = sales.purchase_date_key
            WHERE product.product_id <> 'UNKNOWN'
        ) AS source_sales
    """
    promotions = spark.read.jdbc(
        url,
        promotions_query,
        predicates=hash_predicates("promotion_id"),
        properties=properties,
    )
    sales = spark.read.jdbc(
        url,
        sales_query,
        predicates=hash_predicates("order_id"),
        properties=properties,
    )
    return promotions, sales


def transform(promotions: DataFrame, sales: DataFrame) -> DataFrame:
    """Áp dụng latest active promotion và tính các chỉ số tài chính."""

    version_window = Window.partitionBy("promotion_id").orderBy(
        F.col("version").desc()
    )
    promotions = (
        promotions.withColumn("version_rank", F.row_number().over(version_window))
        .filter(F.col("version_rank") == 1)
        .drop("version_rank")
    )
    joined = sales.join(
        promotions,
        (sales.product_id == promotions.product_id)
        & (F.col("purchase_date") >= F.to_date(F.col("starts_at")))
        & (F.col("purchase_date") <= F.to_date(F.col("ends_at"))),
        "left",
    ).drop(promotions.product_id)

    item_price = F.col("item_price").cast("decimal(12,2)")
    freight = F.col("freight_value").cast("decimal(12,2)")
    discount_value = F.coalesce(F.col("discount_value"), F.lit(0)).cast(
        "decimal(12,2)"
    )
    discount = (
        F.when(
            F.col("discount_type") == "percentage",
            F.least(item_price, item_price * discount_value / F.lit(100)),
        )
        .when(
            F.col("discount_type") == "fixed",
            F.least(item_price, discount_value),
        )
        .otherwise(F.lit(0))
        .cast("decimal(12,2)")
    )
    return joined.select(
        "order_id",
        "item_number",
        "product_id",
        "purchase_date",
        F.coalesce(F.col("promotion_id"), F.lit("NO_PROMOTION")).alias(
            "promotion_id"
        ),
        F.coalesce(F.col("version"), F.lit(0)).cast("integer").alias(
            "promotion_version"
        ),
        "discount_type",
        discount_value.alias("discount_value"),
        item_price.alias("item_price"),
        freight.alias("freight_value"),
        (item_price + freight).cast("decimal(12,2)").alias("gross_amount"),
        discount.alias("discount_amount"),
        (item_price + freight - discount)
        .cast("decimal(12,2)")
        .alias("net_amount_after_discount"),
    )


def prepare_staging(database: DatabaseConfig, run_id: str) -> None:
    with database.connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM week6_curated.sales_promotion_staging WHERE run_id=%s",
                (run_id,),
            )
            cursor.execute(
                "UPDATE week6_control.pipeline_runs SET status='transforming' WHERE run_id=%s",
                (run_id,),
            )


def build_spark(batch_id: str) -> SparkSession:
    master_url = os.getenv(
        "WEEK6_SPARK_MASTER",
        os.getenv("SPARK_MASTER_URL", "local[2]"),
    )
    builder = (
        SparkSession.builder.appName(f"week6-promotion-staging-{batch_id}")
        .master(master_url)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            os.getenv("WEEK6_JDBC_PACKAGE", DEFAULT_JDBC_PACKAGE),
        )
    )
    if master_url.startswith("spark://"):
        builder = builder.config(
            "spark.driver.host",
            os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler"),
        ).config("spark.driver.bindAddress", "0.0.0.0")
    return builder.getOrCreate()


def run(run_id: str, batch_id: str, summary_path: str | None = None) -> dict:
    database = DatabaseConfig.from_env()
    prepare_staging(database, run_id)
    with database.connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT accepted_count FROM week6_control.pipeline_runs WHERE run_id=%s",
            (run_id,),
        )
        audit = cursor.fetchone()
        if audit is None:
            raise RuntimeError(f"Không tìm thấy audit run: {run_id}")
        accepted_count = audit[0]
        if accepted_count == 0:
            cursor.execute("SELECT COUNT(*) FROM week6_curated.sales_promotion")
            curated_count = cursor.fetchone()[0]
            update_counts(run_id, status="transforming", curated_count=curated_count)
            summary = {
                "run_id": run_id,
                "batch_id": batch_id,
                "mode": "no-op",
                "promotion_count": 0,
                "curated_count": curated_count,
            }
            write_summary(summary_path, summary)
            return summary

    spark = build_spark(batch_id)
    try:
        promotions, sales = load_source_dataframes(spark, database)
        result = transform(promotions, sales).withColumn("run_id", F.lit(run_id)).withColumn(
            "batch_id", F.lit(batch_id)
        )
        ordered = result.select(
            "run_id",
            "batch_id",
            "order_id",
            "item_number",
            "product_id",
            "purchase_date",
            "promotion_id",
            "promotion_version",
            "discount_type",
            "discount_value",
            "item_price",
            "freight_value",
            "gross_amount",
            "discount_amount",
            "net_amount_after_discount",
        ).cache()
        curated_count = ordered.count()
        url, properties = jdbc_configuration(database)
        ordered.write.mode("append").jdbc(
            url,
            "week6_curated.sales_promotion_staging",
            properties=properties,
        )
        promotion_count = promotions.count()
        sales_count = sales.count()
        ordered.unpersist()
        update_counts(run_id, status="transforming", curated_count=curated_count)
        summary = {
            "run_id": run_id,
            "batch_id": batch_id,
            "mode": "staged-snapshot",
            "promotion_count": promotion_count,
            "sales_count": sales_count,
            "curated_count": curated_count,
        }
        write_summary(summary_path, summary)
        return summary
    finally:
        spark.stop()


def write_summary(summary_path: str | None, summary: dict) -> None:
    if not summary_path:
        return
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark staging snapshot promotion Tuần 6")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--summary-path")
    args = parser.parse_args()
    print(json.dumps(run(**vars(args)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
