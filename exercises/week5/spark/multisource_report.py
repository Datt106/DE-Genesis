from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def spark_master_url() -> str:
    return (
        os.getenv("WEEK5_SPARK_MASTER")
        or os.getenv("SPARK_MASTER_URL")
        or "local[2]"
    )


def build_reports(spark: SparkSession, manifest: dict):
    sources = manifest["sources"]
    csv_sales = spark.read.option("header", True).option("inferSchema", True).csv(
        sources["csv"]["path"]
    )
    product_sales = spark.read.option("header", True).option("inferSchema", True).csv(
        sources["postgresql"]["path"]
    )
    promotions = spark.read.json(sources["rest_api"]["path"])

    version_window = Window.partitionBy("promotion_id").orderBy(F.col("version").desc())
    active_promotions = (
        promotions.withColumn("version_rank", F.row_number().over(version_window))
        .filter(F.col("version_rank") == 1)
        .drop("version_rank")
    )
    product_report = (
        product_sales.join(active_promotions, "product_id", "left")
        .select(
            "product_id",
            "order_item_count",
            "gross_item_value",
            F.coalesce(F.col("promotion_id"), F.lit("NO_PROMOTION")).alias("promotion_id"),
            "discount_type",
            F.coalesce(F.col("discount_value"), F.lit(0)).alias("discount_value"),
            "starts_at",
            "ends_at",
        )
    )
    regional_report = (
        csv_sales.withColumn(
            "gross_revenue",
            F.col("quantity").cast("decimal(18,2)")
            * F.col("unit_price").cast("decimal(18,2)"),
        )
        .groupBy("region", "category")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("unit_count"),
            F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
        )
    )
    return product_report, regional_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Báo cáo Spark đa nguồn tuần 5")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    spark = (
        SparkSession.builder.appName(f"week5-multisource-{manifest['batch_id']}")
        .master(spark_master_url())
        .getOrCreate()
    )
    try:
        product_report, regional_report = build_reports(spark, manifest)
        output_dir = Path(args.output_root) / manifest["batch_id"] / "report"
        product_path = output_dir / "product_promotions"
        regional_path = output_dir / "regional_sales"
        product_report.write.mode("overwrite").parquet(str(product_path))
        regional_report.write.mode("overwrite").parquet(str(regional_path))
        summary = {
            "batch_id": manifest["batch_id"],
            "source_counts": {
                name: details["count"] for name, details in manifest["sources"].items()
            },
            "report_counts": {
                "product_promotions": product_report.count(),
                "regional_sales": regional_report.count(),
            },
            "outputs": {
                "product_promotions": str(product_path.resolve()),
                "regional_sales": str(regional_path.resolve()),
            },
        }
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
