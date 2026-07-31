from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week5.ingestion import ensure_schema


def compare() -> dict:
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH airflow AS (
                    SELECT order_id,item_number,product_id,promotion_id,promotion_version,
                           gross_amount,discount_amount,net_amount_after_discount
                    FROM week5_curated.sales_promotion_airflow
                ),
                nifi AS (
                    SELECT order_id,item_number,product_id,promotion_id,promotion_version,
                           gross_amount,discount_amount,net_amount_after_discount
                    FROM week5_curated.sales_promotion_nifi
                )
                SELECT
                    (SELECT COUNT(*) FROM airflow),
                    (SELECT COUNT(*) FROM nifi),
                    (SELECT COUNT(*) FROM (
                        (SELECT * FROM airflow EXCEPT SELECT * FROM nifi)
                        UNION ALL
                        (SELECT * FROM nifi EXCEPT SELECT * FROM airflow)
                    ) differences)
                """
            )
            airflow_count, nifi_count, difference_count = cursor.fetchone()
            cursor.execute(
                """
                SELECT source_mode, status, raw_count, accepted_count,
                       rejected_count, curated_count,
                       EXTRACT(EPOCH FROM (finished_at-started_at))::numeric(12,3)
                FROM week5_control.pipeline_runs
                ORDER BY started_at DESC
                """
            )
            runs = [
                {
                    "source_mode": row[0],
                    "status": row[1],
                    "raw_count": row[2],
                    "accepted_count": row[3],
                    "rejected_count": row[4],
                    "curated_count": row[5],
                    "duration_seconds": float(row[6]) if row[6] is not None else None,
                }
                for row in cursor.fetchall()
            ]
    return {
        "airflow_count": airflow_count,
        "nifi_count": nifi_count,
        "difference_count": difference_count,
        "equivalent": difference_count == 0 and airflow_count == nifi_count,
        "runs": runs,
    }


if __name__ == "__main__":
    result = compare()
    path = Path("output/week5/benchmark/comparison.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
