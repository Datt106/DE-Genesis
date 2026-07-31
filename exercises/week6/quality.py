from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from exercises.week5.common import DatabaseConfig
from exercises.week6.repository import update_counts


class DataQualityError(RuntimeError):
    """Một quality gate blocking đã thất bại."""


@dataclass(frozen=True)
class Check:
    name: str
    actual: float
    expected: str
    passed: bool
    details: str


def store_checks(run_id: str, checks: list[Check]) -> None:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        for check in checks:
            cursor.execute(
                """
                INSERT INTO week6_control.quality_results(
                    run_id, check_name, check_status, actual_value, expected_value, details
                ) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id,check_name) DO UPDATE
                SET check_status=EXCLUDED.check_status,
                    actual_value=EXCLUDED.actual_value,
                    expected_value=EXCLUDED.expected_value,
                    details=EXCLUDED.details,
                    checked_at=NOW()
                """,
                (
                    run_id,
                    check.name,
                    "passed" if check.passed else "failed",
                    str(check.actual),
                    check.expected,
                    check.details,
                ),
            )


def raise_for_failures(run_id: str, checks: list[Check]) -> None:
    store_checks(run_id, checks)
    failures = [check.name for check in checks if not check.passed]
    if failures:
        update_counts(run_id, status="quality_failed")
        raise DataQualityError(f"Quality gate thất bại: {', '.join(failures)}")


def run_raw_quality_gate(config: dict[str, Any]) -> dict[str, Any]:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT raw_count, accepted_count, rejected_count
            FROM week6_control.pipeline_runs
            WHERE run_id=%s
            """,
            (config["run_id"],),
        )
        audit = cursor.fetchone()
        if audit is None:
            raise DataQualityError("Không tìm thấy audit run")
        raw_count, accepted_count, rejected_count = audit
        cursor.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE is_valid),
                   COUNT(*) FILTER (WHERE NOT is_valid),
                   COUNT(*) FILTER (
                       WHERE source_updated_at < %s OR source_updated_at >= %s
                   )
            FROM week6_raw.promotions
            WHERE batch_id=%s
            """,
            (config["window_start"], config["window_end"], config["batch_id"]),
        )
        stored_count, stored_valid, stored_invalid, outside_window = cursor.fetchone()

    invalid_rate = rejected_count / raw_count if raw_count else 0.0
    checks = [
        Check(
            "DQ01_audit_reconciliation",
            accepted_count + rejected_count - raw_count,
            "0",
            accepted_count + rejected_count == raw_count,
            "accepted_count + rejected_count phải bằng raw_count",
        ),
        Check(
            "DQ02_raw_storage_reconciliation",
            stored_count - raw_count,
            "0",
            stored_count == raw_count,
            "Số dòng raw đã lưu phải bằng số dòng API trả về",
        ),
        Check(
            "DQ03_validity_reconciliation",
            abs(stored_valid - accepted_count) + abs(stored_invalid - rejected_count),
            "0",
            stored_valid == accepted_count and stored_invalid == rejected_count,
            "Cờ is_valid trong raw phải khớp audit",
        ),
        Check(
            "DQ04_invalid_rate",
            invalid_rate,
            f"<= {config['invalid_rate_threshold']}",
            invalid_rate <= config["invalid_rate_threshold"],
            "Tỷ lệ record lỗi không được vượt ngưỡng cấu hình",
        ),
        Check(
            "DQ05_incremental_window",
            outside_window,
            "0",
            outside_window == 0,
            "source_updated_at phải thuộc cửa sổ [start, end)",
        ),
    ]
    raise_for_failures(config["run_id"], checks)
    return {"passed": True, "checks": len(checks), "invalid_rate": invalid_rate}


def run_curated_quality_gate(config: dict[str, Any]) -> dict[str, Any]:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT accepted_count, curated_count
            FROM week6_control.pipeline_runs
            WHERE run_id=%s
            """,
            (config["run_id"],),
        )
        accepted_count, audited_curated = cursor.fetchone()
        cursor.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) - COUNT(DISTINCT (order_id,item_number,promotion_id,promotion_version)),
                   COUNT(*) FILTER (WHERE discount_amount < 0),
                   COUNT(*) FILTER (WHERE net_amount_after_discount < freight_value)
            FROM week6_curated.sales_promotion
            """
        )
        curated_count, duplicate_count, negative_discount, net_floor_errors = cursor.fetchone()
        cursor.execute(
            "SELECT COUNT(*) FROM olist_olap.fact_sales"
        )
        source_sales_count = cursor.fetchone()[0]

    should_match_snapshot = accepted_count > 0 or curated_count > 0
    checks = [
        Check(
            "DQ06_curated_audit_reconciliation",
            curated_count - audited_curated,
            "0",
            curated_count == audited_curated,
            "Số dòng curated phải khớp audit",
        ),
        Check(
            "DQ07_unique_curated_grain",
            duplicate_count,
            "0",
            duplicate_count == 0,
            "Grain curated không được trùng",
        ),
        Check(
            "DQ08_nonnegative_discount",
            negative_discount,
            "0",
            negative_discount == 0,
            "discount_amount không được âm",
        ),
        Check(
            "DQ09_net_amount_floor",
            net_floor_errors,
            "0",
            net_floor_errors == 0,
            "net_amount_after_discount không được thấp hơn freight_value",
        ),
        Check(
            "DQ10_snapshot_completeness",
            curated_count - source_sales_count if should_match_snapshot else 0,
            "0",
            not should_match_snapshot or curated_count == source_sales_count,
            "Snapshot curated phải có cùng số grain với fact_sales",
        ),
    ]
    raise_for_failures(config["run_id"], checks)
    return {"passed": True, "checks": len(checks), "curated_count": curated_count}
