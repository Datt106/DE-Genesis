from __future__ import annotations

import os
import time
from collections.abc import Iterator
from datetime import datetime

import psycopg2
import requests
from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily


PIPELINE_TABLES = (
    "week5_control.pipeline_runs",
    "week6_control.pipeline_runs",
)
QUALITY_TABLES = (
    ("week5_control.quality_results", "week5_control.pipeline_runs"),
    ("week6_control.quality_results", "week6_control.pipeline_runs"),
)


def database_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "de_roadmap"),
        user=os.getenv("POSTGRES_USER", "de_user"),
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=3,
    )


def epoch(value: datetime | None) -> float:
    return value.timestamp() if value else 0.0


class DeGenesisCollector:
    def collect(self) -> Iterator[GaugeMetricFamily]:
        dependency = GaugeMetricFamily(
            "de_genesis_dependency_up",
            "Trạng thái sẵn sàng của dependency nội bộ",
            labels=["dependency"],
        )
        endpoints = {
            "mock_api": os.getenv("MOCK_API_HEALTH_URL", "http://mock-api:8000/health"),
            "airflow": os.getenv("AIRFLOW_HEALTH_URL", "http://airflow-webserver:8080/health"),
            "spark_master": os.getenv("SPARK_HEALTH_URL", "http://spark-master:8080"),
        }
        for name, url in endpoints.items():
            dependency.add_metric([name], check_http(url))

        try:
            with database_connection() as connection:
                dependency.add_metric(["postgres"], 1)
                yield dependency
                yield from collect_database_metrics(connection)
        except Exception:
            dependency.add_metric(["postgres"], 0)
            yield dependency


def check_http(url: str) -> int:
    try:
        response = requests.get(url, timeout=3)
        return int(response.status_code < 500)
    except requests.RequestException:
        return 0


def collect_database_metrics(connection) -> Iterator[GaugeMetricFamily]:
    run_total = GaugeMetricFamily(
        "de_genesis_pipeline_runs_total",
        "Tổng số pipeline run theo trạng thái",
        labels=["pipeline_name", "status"],
    )
    last_success = GaugeMetricFamily(
        "de_genesis_pipeline_last_success_timestamp_seconds",
        "Unix timestamp của lần chạy thành công gần nhất",
        labels=["pipeline_name"],
    )
    last_success_state = GaugeMetricFamily(
        "de_genesis_pipeline_last_run_success",
        "Run gần nhất thành công là 1, trạng thái khác là 0",
        labels=["pipeline_name"],
    )
    last_duration = GaugeMetricFamily(
        "de_genesis_pipeline_last_duration_seconds",
        "Thời gian xử lý của run gần nhất",
        labels=["pipeline_name"],
    )
    last_rows = GaugeMetricFamily(
        "de_genesis_pipeline_last_rows",
        "Số dòng của run gần nhất theo tầng dữ liệu",
        labels=["pipeline_name", "stage"],
    )
    quality_failures = GaugeMetricFamily(
        "de_genesis_pipeline_last_quality_failures",
        "Số quality check thất bại trong run gần nhất",
        labels=["pipeline_name"],
    )

    existing_pipeline_tables = [
        table for table in PIPELINE_TABLES if relation_exists(connection, table)
    ]
    if not existing_pipeline_tables:
        yield from (
            run_total,
            last_success,
            last_success_state,
            last_duration,
            last_rows,
            quality_failures,
        )
        return

    union = " UNION ALL ".join(
        (
            "SELECT run_id,pipeline_name,status,started_at,finished_at,"
            "raw_count,accepted_count,rejected_count,curated_count "
            f"FROM {table}"
        )
        for table in existing_pipeline_tables
    )
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT pipeline_name,status,COUNT(*)
            FROM ({union}) AS runs
            GROUP BY pipeline_name,status
            """
        )
        for pipeline_name, status, count in cursor.fetchall():
            run_total.add_metric([pipeline_name, status], count)

        cursor.execute(
            f"""
            SELECT DISTINCT ON (pipeline_name)
                run_id,pipeline_name,status,started_at,finished_at,
                raw_count,accepted_count,rejected_count,curated_count
            FROM ({union}) AS runs
            ORDER BY pipeline_name,started_at DESC
            """
        )
        latest = cursor.fetchall()
        for (
            run_id,
            pipeline_name,
            status,
            started_at,
            finished_at,
            raw_count,
            accepted_count,
            rejected_count,
            curated_count,
        ) in latest:
            last_success_state.add_metric([pipeline_name], int(status == "success"))
            duration = (
                (finished_at - started_at).total_seconds()
                if finished_at is not None
                else max(0.0, time.time() - epoch(started_at))
            )
            last_duration.add_metric([pipeline_name], duration)
            for stage, value in (
                ("raw", raw_count),
                ("accepted", accepted_count),
                ("rejected", rejected_count),
                ("curated", curated_count),
            ):
                last_rows.add_metric([pipeline_name, stage], value)
            quality_failures.add_metric(
                [pipeline_name],
                count_quality_failures(connection, pipeline_name, run_id),
            )

        cursor.execute(
            f"""
            SELECT pipeline_name,MAX(finished_at)
            FROM ({union}) AS runs
            WHERE status='success'
            GROUP BY pipeline_name
            """
        )
        for pipeline_name, finished_at in cursor.fetchall():
            last_success.add_metric([pipeline_name], epoch(finished_at))

    yield from (
        run_total,
        last_success,
        last_success_state,
        last_duration,
        last_rows,
        quality_failures,
    )


def relation_exists(connection, relation: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
        return bool(cursor.fetchone()[0])


def count_quality_failures(connection, pipeline_name: str, run_id: str) -> int:
    for quality_table, run_table in QUALITY_TABLES:
        if not relation_exists(connection, quality_table):
            continue
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {quality_table} AS quality
                JOIN {run_table} AS run ON run.run_id=quality.run_id
                WHERE run.pipeline_name=%s
                  AND quality.run_id=%s
                  AND quality.check_status='failed'
                """,
                (pipeline_name, run_id),
            )
            count = cursor.fetchone()[0]
            if count:
                return count
    return 0


def main() -> None:
    REGISTRY.register(DeGenesisCollector())
    start_http_server(int(os.getenv("METRICS_PORT", "9108")))
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
