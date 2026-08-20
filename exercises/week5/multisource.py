from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from exercises.week5.api_contracts import RestAuth
from exercises.week5.common import DatabaseConfig
from exercises.week5.ingestion import fetch_all_promotions


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV = PROJECT_ROOT / "data" / "sample" / "sales.csv"


def extract_multisource_batch(
    *,
    batch_id: str,
    output_root: str | Path,
    csv_path: str | Path = DEFAULT_CSV,
    api_url: str | None = None,
    auth: RestAuth | None = None,
) -> Path:
    """Chụp ba nguồn thành file bất biến để Spark đọc lại được khi retry."""

    batch_dir = Path(output_root) / batch_id / "raw"
    batch_dir.mkdir(parents=True, exist_ok=True)
    csv_source = Path(csv_path)
    if not csv_source.is_file():
        raise FileNotFoundError(f"Không tìm thấy nguồn CSV: {csv_source}")

    csv_snapshot = batch_dir / "sales.csv"
    postgres_snapshot = batch_dir / "postgres_product_sales.csv"
    api_snapshot = batch_dir / "promotions.jsonl"
    shutil.copyfile(csv_source, csv_snapshot)

    database = DatabaseConfig.from_env()
    with database.connect() as connection, connection.cursor() as cursor:
        with postgres_snapshot.open("w", encoding="utf-8", newline="") as output:
            cursor.copy_expert(
                """
                COPY (
                    SELECT product.product_id,
                           COUNT(*) AS order_item_count,
                           SUM(sales.item_price)::numeric(18,2) AS gross_item_value
                    FROM olist_olap.fact_sales AS sales
                    JOIN olist_olap.dim_product AS product
                      ON product.product_key = sales.product_key
                    WHERE product.product_id <> 'UNKNOWN'
                    GROUP BY product.product_id
                    ORDER BY product.product_id
                ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)
                """,
                output,
            )

    promotions = fetch_all_promotions(
        api_url or os.getenv("MOCK_API_URL", "http://localhost:8000"),
        page_size=int(os.getenv("WEEK5_API_PAGE_SIZE", "100")),
        headers=(auth or RestAuth()).headers(),
    )
    with api_snapshot.open("w", encoding="utf-8") as output:
        for record in promotions:
            output.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    with csv_snapshot.open("r", encoding="utf-8") as source:
        csv_count = max(sum(1 for _ in source) - 1, 0)
    with postgres_snapshot.open("r", encoding="utf-8") as source:
        postgres_count = max(sum(1 for _ in source) - 1, 0)
    manifest = {
        "batch_id": batch_id,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "csv": {"path": str(csv_snapshot.resolve()), "count": csv_count},
            "postgresql": {
                "path": str(postgres_snapshot.resolve()),
                "count": postgres_count,
            },
            "rest_api": {"path": str(api_snapshot.resolve()), "count": len(promotions)},
        },
    }
    manifest_path = batch_dir.parent / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Chụp nguồn CSV, PostgreSQL và REST API tuần 5")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--output-root", default="output/week5/multisource")
    parser.add_argument("--csv-path", default=str(DEFAULT_CSV))
    parser.add_argument("--api-url")
    args = parser.parse_args()
    print(
        extract_multisource_batch(
            batch_id=args.batch_id,
            output_root=args.output_root,
            csv_path=args.csv_path,
            api_url=args.api_url,
        )
    )


if __name__ == "__main__":
    main()
