from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_PRODUCT_IDS = [f"product-{number:05d}" for number in range(1, 251)]


def load_product_ids() -> list[str]:
    path = Path(os.getenv("PROMOTION_SEED_FILE", ""))
    if not path.is_file():
        return DEFAULT_PRODUCT_IDS
    with path.open("r", encoding="utf-8", newline="") as source:
        values = [
            row["product_id"].strip()
            for row in csv.DictReader(source)
            if row.get("product_id", "").strip()
        ]
    return values[:250] or DEFAULT_PRODUCT_IDS


def build_promotions() -> list[dict]:
    promotions = []
    updated_at = datetime(2026, 7, 20, 10, 0, tzinfo=timezone.utc)
    for index, product_id in enumerate(load_product_ids(), start=1):
        discount_type = "percentage" if index % 3 else "fixed"
        discount_value = float(5 + index % 21) if discount_type == "percentage" else float(2 + index % 9)
        promotions.append(
            {
                "promotion_id": f"PROMO-{index:06d}",
                "product_id": product_id,
                "promotion_name": f"Khuyến mại sản phẩm {index:03d}",
                "discount_type": discount_type,
                "discount_value": discount_value,
                "starts_at": "2016-01-01T00:00:00+00:00",
                "ends_at": "2019-12-31T23:59:59+00:00",
                "status": "active",
                "version": 1,
                "updated_at": updated_at.isoformat(),
            }
        )
    return promotions
