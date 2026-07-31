from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from psycopg2.extras import execute_values
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week5.spark.transform_promotions import transform
from exercises.week6.repository import ensure_schema, update_counts


def load_snapshot_rows(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (promotion_id)
                payload->>'promotion_id',
                payload->>'product_id',
                payload->>'discount_type',
                (payload->>'discount_value')::numeric,
                (payload->>'starts_at')::timestamptz,
                (payload->>'ends_at')::timestamptz,
                (payload->>'version')::integer
            FROM week6_raw.promotions
            WHERE is_valid
            ORDER BY promotion_id, source_updated_at DESC, ingested_at DESC
            """
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


def replace_curated(connection, result, *, run_id: str, batch_id: str) -> int:
    rows = [
        (
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
        cursor.execute("DELETE FROM week6_curated.sales_promotion")
        if rows:
            execute_values(
                cursor,
                """
                INSERT INTO week6_curated.sales_promotion(
                    run_id, batch_id, order_id, item_number, product_id,
                    purchase_date, promotion_id, promotion_version, discount_type,
                    discount_value, item_price, freight_value, gross_amount,
                    discount_amount, net_amount_after_discount
                ) VALUES %s
                """,
                rows,
                page_size=1000,
            )
    return len(rows)


def run(run_id: str, batch_id: str, summary_path: str | None = None) -> dict:
    database = DatabaseConfig.from_env()
    with database.connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT accepted_count FROM week6_control.pipeline_runs WHERE run_id=%s",
                (run_id,),
            )
            audit = cursor.fetchone()
            if audit is None:
                raise RuntimeError(f"Không tìm thấy audit run: {run_id}")
            accepted_count = audit[0]
            cursor.execute(
                "UPDATE week6_control.pipeline_runs SET status='transforming' WHERE run_id=%s",
                (run_id,),
            )
            if accepted_count == 0:
                cursor.execute("SELECT COUNT(*) FROM week6_curated.sales_promotion")
                curated_count = cursor.fetchone()[0]
                cursor.execute(
                    """
                    UPDATE week6_control.pipeline_runs
                    SET status='transforming', curated_count=%s
                    WHERE run_id=%s
                    """,
                    (curated_count, run_id),
                )
                summary = {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "mode": "no-op",
                    "promotion_count": 0,
                    "curated_count": curated_count,
                }
                write_summary(summary_path, summary)
                return summary
            promotions, sales = load_snapshot_rows(connection)

    master_url = os.getenv(
        "WEEK6_SPARK_MASTER",
        os.getenv("SPARK_MASTER_URL", "local[2]"),
    )
    builder = SparkSession.builder.appName(f"week6-production-{batch_id}").master(
        master_url
    )
    if master_url.startswith("spark://"):
        builder = builder.config(
            "spark.driver.host",
            os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler"),
        ).config("spark.driver.bindAddress", "0.0.0.0")
    spark = builder.getOrCreate()
    try:
        result = transform(spark, promotions, sales)
        with database.connect() as connection:
            curated_count = replace_curated(
                connection,
                result,
                run_id=run_id,
                batch_id=batch_id,
            )
        update_counts(run_id, status="transforming", curated_count=curated_count)
        summary = {
            "run_id": run_id,
            "batch_id": batch_id,
            "mode": "snapshot-refresh",
            "promotion_count": len(promotions),
            "sales_count": len(sales),
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
    parser = argparse.ArgumentParser(description="Spark snapshot production tuần 6")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--summary-path")
    args = parser.parse_args()
    print(json.dumps(run(**vars(args)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
