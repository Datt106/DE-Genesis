"""Blocking quality gate cho log report staging trước atomic publish."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from exercises.week5.common import DatabaseConfig


class LogQualityError(RuntimeError):
    """Log report không đạt hợp đồng dữ liệu."""


@dataclass(frozen=True)
class LogCheck:
    name: str
    actual: float
    expected: str
    passed: bool
    details: str


def record_log_artifact_quality(
    run_id: str,
    *,
    passed: bool,
    details: str,
) -> None:
    """Ghi DQ artifact để bước publish không thể bỏ qua kiểm tra HDFS."""

    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO week6_control.log_quality_results(
                run_id, check_name, check_status, actual_value,
                expected_value, details
            ) VALUES (%s,'LOG_DQ08_HDFS_STAGING',%s,%s,'2 _SUCCESS',%s)
            ON CONFLICT (run_id,check_name) DO UPDATE
            SET check_status=EXCLUDED.check_status,
                actual_value=EXCLUDED.actual_value,
                expected_value=EXCLUDED.expected_value,
                details=EXCLUDED.details,
                checked_at=NOW()
            """,
            (
                run_id,
                "passed" if passed else "failed",
                "2" if passed else "missing",
                details[:4000],
            ),
        )
        if not passed:
            cursor.execute(
                """
                UPDATE week6_control.log_report_runs
                SET status='quality_failed'
                WHERE run_id=%s
                """,
                (run_id,),
            )


def run_log_report_quality_gate(config: dict[str, Any]) -> dict[str, Any]:
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT source_count, minute_report_count, status_report_count
            FROM week6_control.log_report_runs
            WHERE run_id=%s
            """,
            (config["run_id"],),
        )
        audit = cursor.fetchone()
        if audit is None:
            raise LogQualityError("Không tìm thấy audit log report")
        source_count, audited_minute_count, audited_status_count = audit

        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(request_count), 0),
                   COUNT(*) - COUNT(DISTINCT (minute_start,service)),
                   COUNT(*) FILTER (
                       WHERE minute_start < %s OR minute_start >= %s
                   )
            FROM week6_log.requests_per_minute_staging
            WHERE run_id=%s
            """,
            (config["window_start"], config["window_end"], config["run_id"]),
        )
        minute_count, minute_requests, minute_duplicates, minute_outside = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(request_count), 0),
                   COUNT(*) - COUNT(DISTINCT (minute_start,service,status_code)),
                   COUNT(*) FILTER (WHERE status_code < 100 OR status_code > 599)
            FROM week6_log.status_distribution_staging
            WHERE run_id=%s
            """,
            (config["run_id"],),
        )
        status_count, status_requests, status_duplicates, bad_status = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT minute_start, service
                FROM week6_log.status_distribution_staging
                WHERE run_id=%s
                GROUP BY minute_start, service
                HAVING ABS(SUM(percentage) - 100) > 0.01
            ) AS invalid_percentage_groups
            """,
            (config["run_id"],),
        )
        invalid_percentage_groups = cursor.fetchone()[0]

        checks = [
            LogCheck(
                "LOG_DQ01_minute_audit",
                minute_count - audited_minute_count,
                "0",
                minute_count == audited_minute_count,
                "Số grain requests/phút trong staging phải khớp audit",
            ),
            LogCheck(
                "LOG_DQ02_status_audit",
                status_count - audited_status_count,
                "0",
                status_count == audited_status_count,
                "Số grain status trong staging phải khớp audit",
            ),
            LogCheck(
                "LOG_DQ03_request_reconciliation",
                minute_requests - source_count,
                "0",
                minute_requests == source_count,
                "Tổng request theo phút phải bằng số event hợp lệ",
            ),
            LogCheck(
                "LOG_DQ04_status_reconciliation",
                status_requests - source_count,
                "0",
                status_requests == source_count,
                "Tổng request theo status phải bằng số event hợp lệ",
            ),
            LogCheck(
                "LOG_DQ05_unique_grain",
                minute_duplicates + status_duplicates,
                "0",
                minute_duplicates == 0 and status_duplicates == 0,
                "Hai report không được trùng grain",
            ),
            LogCheck(
                "LOG_DQ06_valid_domain",
                bad_status + minute_outside,
                "0",
                bad_status == 0 and minute_outside == 0,
                "Status phải hợp lệ và minute_start phải thuộc cửa sổ",
            ),
            LogCheck(
                "LOG_DQ07_status_percentage",
                invalid_percentage_groups,
                "0",
                invalid_percentage_groups == 0,
                "Phân bố status của mỗi phút/service phải cộng thành 100%",
            ),
        ]
        for check in checks:
            cursor.execute(
                """
                INSERT INTO week6_control.log_quality_results(
                    run_id, check_name, check_status, actual_value,
                    expected_value, details
                ) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id,check_name) DO UPDATE
                SET check_status=EXCLUDED.check_status,
                    actual_value=EXCLUDED.actual_value,
                    expected_value=EXCLUDED.expected_value,
                    details=EXCLUDED.details,
                    checked_at=NOW()
                """,
                (
                    config["run_id"],
                    check.name,
                    "passed" if check.passed else "failed",
                    str(check.actual),
                    check.expected,
                    check.details,
                ),
            )
        failures = [check.name for check in checks if not check.passed]
        if failures:
            cursor.execute(
                """
                UPDATE week6_control.log_report_runs
                SET status='quality_failed'
                WHERE run_id=%s
                """,
                (config["run_id"],),
            )

    if failures:
        raise LogQualityError(f"Log quality gate thất bại: {', '.join(failures)}")
    return {"passed": True, "checks": len(checks)}
