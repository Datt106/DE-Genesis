from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week5.ingestion import ensure_schema


PROMOTION_SCHEMA = StructType(
    [
        StructField("promotion_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("discount_type", StringType(), True),
        StructField("discount_value", DecimalType(12, 2), False),
        StructField("starts_at", TimestampType(), False),
        StructField("ends_at", TimestampType(), False),
        StructField("version", IntegerType(), False),
    ]
)

SALES_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("item_number", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("purchase_date", DateType(), True),
        StructField("item_price", DecimalType(12, 2), False),
        StructField("freight_value", DecimalType(12, 2), False),
    ]
)


def load_source_rows(connection, source_mode: str, batch_id: str):
    raw_table = f"week5_raw.promotions_{source_mode}"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                payload->>'promotion_id',
                payload->>'product_id',
                payload->>'discount_type',
                (payload->>'discount_value')::numeric,
                (payload->>'starts_at')::timestamptz,
                (payload->>'ends_at')::timestamptz,
                (payload->>'version')::integer
            FROM {raw_table}
            WHERE batch_id = %s AND is_valid
            """,
            (batch_id,),
        )
        promotions = cursor.fetchall()
        cursor.execute(
            """
            SELECT
                sales.order_id,
                sales.item_number,
                product.product_id,
                purchase_date.full_date,
                sales.item_price,
                sales.freight_value
            FROM olist_olap.fact_sales AS sales
            JOIN olist_olap.dim_product AS product
              ON product.product_key = sales.product_key
            LEFT JOIN olist_olap.dim_date AS purchase_date
              ON purchase_date.date_key = sales.purchase_date_key
            WHERE product.product_id <> 'UNKNOWN'
            """
        )
        sales = cursor.fetchall()
    return promotions, sales


def transform(spark: SparkSession, promotions: list[tuple], sales: list[tuple]):
    promotion_df = spark.createDataFrame(promotions, PROMOTION_SCHEMA)
    sales_df = spark.createDataFrame(sales, SALES_SCHEMA)
    version_window = Window.partitionBy("promotion_id").orderBy(F.col("version").desc())
    promotion_df = (
        promotion_df.withColumn("version_rank", F.row_number().over(version_window))
        .filter(F.col("version_rank") == 1)
        .drop("version_rank")
    )
    joined = sales_df.join(
        promotion_df,
        (sales_df.product_id == promotion_df.product_id)
        & (F.col("purchase_date") >= F.to_date(F.col("starts_at")))
        & (F.col("purchase_date") <= F.to_date(F.col("ends_at"))),
        "left",
    ).drop(promotion_df.product_id)
    item_price = F.col("item_price").cast("decimal(12,2)")
    discount_value = F.coalesce(F.col("discount_value"), F.lit(0)).cast("decimal(12,2)")
    discount = (
        F.when(
            F.col("discount_type") == "percentage",
            F.least(item_price, item_price * discount_value / F.lit(100)),
        )
        .when(F.col("discount_type") == "fixed", F.least(item_price, discount_value))
        .otherwise(F.lit(0))
        .cast("decimal(12,2)")
    )
    return joined.select(
        "order_id",
        "item_number",
        "product_id",
        "purchase_date",
        F.coalesce(F.col("promotion_id"), F.lit("NO_PROMOTION")).alias("promotion_id"),
        F.coalesce(F.col("version"), F.lit(0)).alias("promotion_version"),
        "discount_type",
        discount_value.alias("discount_value"),
        item_price.alias("item_price"),
        F.col("freight_value").cast("decimal(12,2)").alias("freight_value"),
        (item_price + F.col("freight_value")).cast("decimal(12,2)").alias("gross_amount"),
        discount.alias("discount_amount"),
        (item_price + F.col("freight_value") - discount)
        .cast("decimal(12,2)")
        .alias("net_amount_after_discount"),
    )


def write_curated(connection, result, *, source_mode: str, run_id: str, batch_id: str) -> int:
    target = f"week5_curated.sales_promotion_{source_mode}"
    rows = [
        (
            source_mode,
            run_id,
            batch_id,
            row.order_id,
            row.item_number,
            row.product_id,
            row.purchase_date,
            row.promotion_id,
            row.promotion_version,
            row.discount_type,
            row.discount_value,
            row.item_price,
            row.freight_value,
            row.gross_amount,
            row.discount_amount,
            row.net_amount_after_discount,
        )
        for row in result.toLocalIterator()
    ]
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            f"""
            INSERT INTO {target}(
                source_mode, run_id, batch_id, order_id, item_number, product_id,
                purchase_date, promotion_id, promotion_version, discount_type,
                discount_value, item_price, freight_value, gross_amount,
                discount_amount, net_amount_after_discount
            ) VALUES %s
            ON CONFLICT (
                source_mode, order_id, item_number, promotion_id, promotion_version
            ) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                batch_id = EXCLUDED.batch_id,
                discount_type = EXCLUDED.discount_type,
                discount_value = EXCLUDED.discount_value,
                item_price = EXCLUDED.item_price,
                freight_value = EXCLUDED.freight_value,
                gross_amount = EXCLUDED.gross_amount,
                discount_amount = EXCLUDED.discount_amount,
                net_amount_after_discount = EXCLUDED.net_amount_after_discount,
                processed_at = NOW()
            """,
            rows,
            page_size=1000,
        )
    return len(rows)


def run_quality_checks(connection, *, source_mode: str, run_id: str, batch_id: str) -> dict:
    raw_table = f"week5_raw.promotions_{source_mode}"
    curated_table = f"week5_curated.sales_promotion_{source_mode}"
    checks = [
        ("DQ01_required_keys", f"SELECT COUNT(*) FROM {raw_table} WHERE batch_id=%s AND (promotion_id IS NULL OR product_id IS NULL)", 0),
        ("DQ06_valid_intervals", f"SELECT COUNT(*) FROM {raw_table} WHERE batch_id=%s AND is_valid AND (payload->>'starts_at')::timestamptz > (payload->>'ends_at')::timestamptz", 0),
        ("DQ10_unique_grain", f"SELECT COUNT(*) - COUNT(DISTINCT (order_id,item_number,promotion_id,promotion_version)) FROM {curated_table}", 0),
        ("DQ11_nonnegative_discount", f"SELECT COUNT(*) FROM {curated_table} WHERE discount_amount < 0", 0),
        ("DQ12_net_floor", f"SELECT COUNT(*) FROM {curated_table} WHERE net_amount_after_discount < freight_value", 0),
    ]
    failures = []
    with connection.cursor() as cursor:
        for name, query, expected in checks:
            cursor.execute(query, (batch_id,) if "%s" in query else None)
            actual = cursor.fetchone()[0]
            status = "passed" if actual == expected else "failed"
            failures.extend([name] if status == "failed" else [])
            cursor.execute(
                """
                INSERT INTO week5_control.quality_results(
                    run_id, check_name, check_status, actual_value, expected_value
                ) VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (run_id,check_name) DO UPDATE
                SET check_status=EXCLUDED.check_status,
                    actual_value=EXCLUDED.actual_value,
                    expected_value=EXCLUDED.expected_value,
                    checked_at=NOW()
                """,
                (run_id, name, status, str(actual), str(expected)),
            )
    return {"passed": not failures, "failures": failures, "checks": len(checks)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark core dùng chung cho hai pipeline tuần 5")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--source-mode", required=True, choices=("airflow", "nifi"))
    parser.add_argument("--summary-path")
    args = parser.parse_args()
    database = DatabaseConfig.from_env()
    spark = (
        SparkSession.builder.appName(f"week5-{args.source_mode}-{args.batch_id}")
        .master(
            os.getenv("WEEK5_SPARK_MASTER")
            or os.getenv("SPARK_MASTER_URL")
            or "local[2]"
        )
        .getOrCreate()
    )
    try:
        with database.connect() as connection:
            ensure_schema(connection)
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE week5_control.pipeline_runs SET status='transforming' WHERE run_id=%s",
                    (args.run_id,),
                )
            promotions, sales = load_source_rows(connection, args.source_mode, args.batch_id)
            result = transform(spark, promotions, sales)
            curated_count = write_curated(
                connection,
                result,
                source_mode=args.source_mode,
                run_id=args.run_id,
                batch_id=args.batch_id,
            )
            quality = run_quality_checks(
                connection,
                source_mode=args.source_mode,
                run_id=args.run_id,
                batch_id=args.batch_id,
            )
            status = "success" if quality["passed"] else "quality_failed"
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE week5_control.pipeline_runs
                    SET status=%s, curated_count=%s, finished_at=NOW()
                    WHERE run_id=%s
                    """,
                    (status, curated_count, args.run_id),
                )
                if quality["passed"]:
                    cursor.execute(
                        """
                        INSERT INTO week5_control.ingestion_watermarks(
                            pipeline_name, source_mode, previous_watermark, current_watermark
                        ) VALUES (%s,%s,NULL,NOW())
                        ON CONFLICT (pipeline_name,source_mode) DO UPDATE
                        SET previous_watermark=week5_control.ingestion_watermarks.current_watermark,
                            current_watermark=EXCLUDED.current_watermark,
                            updated_at=NOW()
                        """,
                        (f"week5_{args.source_mode}", args.source_mode),
                    )
            summary = {
                "run_id": args.run_id,
                "batch_id": args.batch_id,
                "source_mode": args.source_mode,
                "promotion_count": len(promotions),
                "sales_count": len(sales),
                "curated_count": curated_count,
                "quality": quality,
            }
            if args.summary_path:
                path = Path(args.summary_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps(summary, ensure_ascii=False))
            return 0 if quality["passed"] else 2
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
