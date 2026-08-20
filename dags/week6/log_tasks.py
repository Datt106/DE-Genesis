"""Airflow callables cho report/backfill/health của pipeline log Tuần 6."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

import requests
from airflow.operators.python import get_current_context


PROJECT_ROOT = Path("/workspace")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week6.log_artifacts import (
    ReportArtifactPaths,
    WebHdfsClient,
    publish_staged_report,
    report_artifact_paths,
    validate_staged_report,
)
from exercises.week6.log_contracts import (
    align_log_report_window_to_watermark,
    resolve_log_report_configuration,
)
from exercises.week6.log_quality import (
    record_log_artifact_quality,
    run_log_report_quality_gate,
)
from exercises.week6.log_repository import (
    assert_log_report_quality_ready,
    initialize_log_report,
    get_log_report_watermark,
    latest_stream_health,
    mark_log_report_failure,
    publish_log_report,
)


def resolve_log_configuration() -> dict:
    context = get_current_context()
    dag_run = context["dag_run"]
    config = resolve_log_report_configuration(
        run_id=dag_run.run_id,
        conf=dag_run.conf or {},
        data_interval_start=context["data_interval_start"],
        data_interval_end=context["data_interval_end"],
        settlement_seconds=int(
            os.getenv("WEEK6_LOG_REPORT_SETTLEMENT_SECONDS", "180")
        ),
        max_event_delay_seconds=int(
            os.getenv("WEEK6_LOG_MAX_EVENT_DELAY_SECONDS", "120")
        ),
        micro_batch_seconds=int(
            os.getenv("WEEK6_LOG_MICRO_BATCH_SECONDS", "30")
        ),
    )
    aligned = align_log_report_window_to_watermark(
        config,
        get_log_report_watermark(),
    )
    artifact_paths = report_artifact_paths(
        run_id=aligned.run_id,
        staging_base_uri=os.getenv(
            "WEEK6_LOG_REPORT_STAGING_PATH",
            "hdfs://namenode:9000/data/week6/reports/_staging/closed",
        ),
        published_base_uri=os.getenv(
            "WEEK6_LOG_CLOSED_REPORT_PATH",
            "hdfs://namenode:9000/data/week6/reports/closed",
        ),
    )
    result = aligned.to_dict()
    result.update(
        hdfs_staging_path=artifact_paths.staging_uri,
        hdfs_published_path=artifact_paths.published_uri,
    )
    return result


def initialize_log_audit() -> None:
    initialize_log_report(pull_log_configuration())


def check_log_dependencies() -> None:
    endpoints = {
        "namenode": os.getenv(
            "WEEK6_NAMENODE_HEALTH_URL",
            "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus",
        ),
        "spark-master": os.getenv(
            "WEEK6_SPARK_HEALTH_URL", "http://spark-master:8080"
        ),
    }
    for name, url in endpoints.items():
        response = requests.get(url, timeout=5)
        if response.status_code >= 400:
            raise RuntimeError(f"Dependency {name} lỗi HTTP {response.status_code}")

    kafka_host = os.getenv("WEEK6_KAFKA_HOST", "kafka")
    kafka_port = int(os.getenv("WEEK6_KAFKA_PORT", "29092"))
    with socket.create_connection((kafka_host, kafka_port), timeout=5):
        pass
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        if cursor.fetchone()[0] != 1:
            raise RuntimeError("PostgreSQL không sẵn sàng")


def check_log_stream_health() -> dict:
    return latest_stream_health(
        max_age_seconds=int(os.getenv("WEEK6_LOG_STREAM_HEALTH_MAX_AGE_SECONDS", "90")),
        max_running_seconds=int(
            os.getenv("WEEK6_LOG_STREAM_MAX_RUNNING_SECONDS", "120")
        ),
    )


def run_spark_log_report() -> None:
    config = pull_log_configuration()
    safe_run_id = config["run_id"].replace(":", "_").replace("/", "_")
    command = [
        sys.executable,
        "-m",
        "exercises.week6.spark.report_service_logs",
        "--run-id",
        config["run_id"],
        "--window-start",
        config["window_start"],
        "--window-end",
        config["window_end"],
        "--raw-path",
        os.getenv(
            "WEEK6_LOG_RAW_PATH",
            "hdfs://namenode:9000/data/week6/raw/service-logs",
        ),
        "--report-staging-path",
        config["hdfs_staging_path"],
        "--summary-path",
        f"/workspace/output/week6/log-verification/{safe_run_id}.json",
    ]
    subprocess.run(
        command,
        check=True,
        cwd="/workspace",
        env=os.environ.copy(),
    )


def quality_gate_log_report() -> dict:
    config = pull_log_configuration()
    database_result = run_log_report_quality_gate(config)
    client = WebHdfsClient(
        os.getenv("WEEK6_NAMENODE_WEBHDFS_URL", "http://namenode:9870"),
        user_name=os.getenv("WEEK6_HDFS_USER", "root"),
    )
    try:
        artifact_result = validate_staged_report(
            client,
            config["hdfs_staging_path"],
        )
    except Exception as exc:
        record_log_artifact_quality(
            config["run_id"],
            passed=False,
            details=str(exc),
        )
        raise
    record_log_artifact_quality(
        config["run_id"],
        passed=True,
        details="Đã tìm thấy _SUCCESS của cả hai dataset trong HDFS staging",
    )
    return {
        "passed": True,
        "checks": database_result["checks"] + 1,
        "artifacts": artifact_result["artifacts"],
    }


def publish_log_report_task() -> dict:
    config = pull_log_configuration()
    assert_log_report_quality_ready(config["run_id"])
    client = WebHdfsClient(
        os.getenv("WEEK6_NAMENODE_WEBHDFS_URL", "http://namenode:9870"),
        user_name=os.getenv("WEEK6_HDFS_USER", "root"),
    )
    artifact_result = publish_staged_report(
        client,
        ReportArtifactPaths(
            staging_uri=config["hdfs_staging_path"],
            published_uri=config["hdfs_published_path"],
        ),
    )
    database_result = publish_log_report(config)
    return {**database_result, **artifact_result}


def pull_log_configuration() -> dict:
    context = get_current_context()
    config = context["ti"].xcom_pull(task_ids="resolve_log_configuration")
    if not isinstance(config, dict):
        raise RuntimeError("Không đọc được log configuration từ XCom")
    return config


def log_failure_callback(context) -> None:
    dag_run = context.get("dag_run")
    if dag_run is None:
        return
    try:
        mark_log_report_failure(
            dag_run.run_id,
            str(context.get("exception") or "Task log thất bại"),
        )
    except Exception:
        # Callback không che khuất exception gốc.
        pass
