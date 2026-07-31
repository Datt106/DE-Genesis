from __future__ import annotations

import asyncio
import math
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, Response

from .models import PromotionPage
from .seed import build_promotions


app = FastAPI(
    title="DE Genesis Promotion API",
    version="1.0.0",
    description="API mô phỏng có tính xác định cho bài thực hành Data Engineering tuần 5.",
)
PROMOTIONS = build_promotions()
TRANSIENT_COUNTS: dict[str, int] = {}


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "records": len(PROMOTIONS)}


@app.post("/admin/reset")
def reset_scenarios() -> dict:
    TRANSIENT_COUNTS.clear()
    return {"status": "reset"}


@app.get("/api/v1/promotions", response_model=PromotionPage)
async def promotions(
    response: Response,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    updated_since: datetime | None = None,
    updated_before: datetime | None = None,
    product_id: str | None = None,
    status: str | None = None,
    scenario: str = "success",
) -> dict:
    if scenario == "rate_limit":
        raise HTTPException(status_code=429, detail="Giới hạn tần suất mô phỏng")
    if scenario == "server_error":
        raise HTTPException(status_code=500, detail="Lỗi máy chủ mô phỏng")
    if scenario == "transient_500":
        key = f"{page}:{page_size}"
        TRANSIENT_COUNTS[key] = TRANSIENT_COUNTS.get(key, 0) + 1
        if TRANSIENT_COUNTS[key] == 1:
            raise HTTPException(status_code=500, detail="Lỗi tạm thời ở lần gọi đầu")
    if scenario == "timeout":
        await asyncio.sleep(15)
    if scenario == "malformed_json":
        return Response(content='{"data": [', media_type="application/json")

    records = list(PROMOTIONS)
    if updated_since:
        records = [row for row in records if datetime.fromisoformat(row["updated_at"]) >= updated_since]
    if updated_before:
        records = [row for row in records if datetime.fromisoformat(row["updated_at"]) < updated_before]
    if product_id:
        records = [row for row in records if row["product_id"] == product_id]
    if status:
        records = [row for row in records if row["status"] == status]
    if scenario == "empty":
        records = []
    elif scenario == "invalid_record" and records:
        records[0] = {**records[0], "product_id": ""}
    elif scenario == "duplicate" and records:
        records.insert(1, dict(records[0]))

    total = len(records)
    total_pages = math.ceil(total / page_size) if total else 0
    start = (page - 1) * page_size
    data = records[start : start + page_size]
    response.headers["X-Mock-Scenario"] = scenario
    return {
        "data": data,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_records": total,
            "has_next": page < total_pages,
        },
    }
