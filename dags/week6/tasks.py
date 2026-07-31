from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import requests
from airflow.operators.python import get_current_context


PROJECT_ROOT = Path("/workspace")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig
from exercises.week6.config import resolve_run_configuration
from exercises.week6.ingestion import ingest_incremental_batch
from exercises.week6.quality import run_curated_quality_gate, run_raw_quality_gate
from exercises.week6.repository import (
    finalize_success,
    initialize_run,
    mark_failure,
)


PIPELINE_NAME = "de_genesis_week6_production_pipeline"


def resolve_configuration() -> dict:
    context = get_current_context()
    dag_run = context["dag_run"]
    config = resolve_run_configuration(
        run_id=dag_run.run_id,
        conf=dag_run.conf or {},
        data_interval_start=context["data_interval_start"],
        data_interval_end=context["data_interval_end"],
        default_invalid_rate_threshold=float(
            os.getenv("WEEK6_INVALID_RATE_THRESHOLD", "0")
        ),
    )
    return config.to_dict()


def initialize_audit() -> None:
    config = pull_configuration()
    initialize_run(config, PIPELINE_NAME)


def check_dependencies() -> None:
    checks = {
        "mock-api": f"{os.environ['MOCK_API_URL'].rstrip('/')}/health",
        "spark-master": "http://spark-master:8080",
    }
    for name, url in checks.items():
        response = requests.get(url, timeout=5)
        if response.status_code >= 400:
            raise RuntimeError(f"Dependency {name} không sẵn sàng: HTTP {response.status_code}")
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        if cursor.fetchone()[0] != 1:
            raise RuntimeError("PostgreSQL không sẵn sàng")


def ingest_incremental() -> dict:
    return ingest_incremental_batch(
        pull_configuration(),
        api_url=os.environ["MOCK_API_URL"],
        page_size=int(os.getenv("WEEK5_API_PAGE_SIZE", "100")),
    )


def quality_gate_raw() -> dict:
    return run_raw_quality_gate(pull_configuration())


def run_spark_snapshot() -> None:
    config = pull_configuration()
    summary_path = (
        f"/workspace/output/week6/verification/"
        f"{config['batch_id']}-{config['run_id'].replace(':', '_')}.json"
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "exercises.week6.spark.transform_promotions",
            "--run-id",
            config["run_id"],
            "--batch-id",
            config["batch_id"],
            "--summary-path",
            summary_path,
        ],
        check=True,
        env=os.environ.copy(),
        cwd="/workspace",
    )


def quality_gate_curated() -> dict:
    return run_curated_quality_gate(pull_configuration())


def finish_run() -> None:
    finalize_success(pull_configuration())


def pull_configuration() -> dict:
    context = get_current_context()
    config = context["ti"].xcom_pull(task_ids="resolve_configuration")
    if not isinstance(config, dict):
        raise RuntimeError("Không đọc được run configuration từ XCom")
    return config


def failure_callback(context) -> None:
    dag_run = context.get("dag_run")
    if not dag_run:
        return
    try:
        mark_failure(dag_run.run_id, str(context.get("exception") or "Task thất bại"))
    except Exception:
        # Callback không được che khuất exception gốc.
        pass
