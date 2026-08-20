from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context


PROJECT_ROOT = Path("/workspace")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from exercises.week5.common import DatabaseConfig  # noqa: E402
from exercises.week5.ingestion import ensure_schema, ingest_airflow_batch, mark_failure  # noqa: E402
from exercises.week5.multisource import extract_multisource_batch  # noqa: E402


def context_ids(source_mode: str) -> tuple[str, str]:
    context = get_current_context()
    dag_run = context["dag_run"]
    run_id = dag_run.run_id
    configured_batch = (dag_run.conf or {}).get("batch_id")
    batch_id = configured_batch or f"{source_mode}-{context['ds_nodash']}-{context['ts_nodash']}"
    return run_id, str(batch_id)


def check_dependencies() -> None:
    import requests

    response = requests.get(f"{os.environ['MOCK_API_URL'].rstrip('/')}/health", timeout=5)
    response.raise_for_status()
    with DatabaseConfig.from_env().connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1


def ingest_airflow() -> dict:
    context = get_current_context()
    run_id, batch_id = context_ids("airflow")
    scenario = (context["dag_run"].conf or {}).get("scenario", "success")
    return ingest_airflow_batch(run_id=run_id, batch_id=batch_id, scenario=scenario)


def prepare_nifi_run() -> dict:
    context = get_current_context()
    conf = context["dag_run"].conf or {}
    required = {"batch_id", "source_mode", "raw_table"}
    missing = sorted(required - set(conf))
    if missing:
        raise AirflowFailException(f"Thiếu dag_run.conf: {', '.join(missing)}")
    if conf["source_mode"] != "nifi" or conf["raw_table"] != "week5_raw.promotions_nifi":
        raise AirflowFailException("source_mode hoặc raw_table không nằm trong allow-list")
    run_id, batch_id = context_ids("nifi")
    with DatabaseConfig.from_env().connect() as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_valid), COUNT(*) FILTER (WHERE NOT is_valid) "
                "FROM week5_raw.promotions_nifi WHERE batch_id=%s",
                (batch_id,),
            )
            raw_count, accepted_count, rejected_count = cursor.fetchone()
            if raw_count == 0:
                raise AirflowFailException(f"Batch NiFi chưa có dữ liệu: {batch_id}")
            source_count = int(conf.get("source_count", raw_count))
            if source_count < raw_count:
                raise AirflowFailException("source_count không được nhỏ hơn số dòng raw")
            cursor.execute(
                """
                INSERT INTO week5_control.pipeline_runs(
                    run_id,pipeline_name,source_mode,batch_id,status,
                    source_count,inserted_count,duplicate_count,
                    raw_count,accepted_count,rejected_count
                ) VALUES (%s,%s,'nifi',%s,'raw_ready',%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id) DO UPDATE
                SET pipeline_name=EXCLUDED.pipeline_name,
                    source_mode=EXCLUDED.source_mode,
                    batch_id=EXCLUDED.batch_id,
                    status='raw_ready',
                    source_count=EXCLUDED.source_count,
                    inserted_count=EXCLUDED.inserted_count,
                    duplicate_count=EXCLUDED.duplicate_count,
                    raw_count=EXCLUDED.raw_count,
                    accepted_count=EXCLUDED.accepted_count,
                    rejected_count=EXCLUDED.rejected_count,
                    started_at=NOW(), finished_at=NULL, error_message=NULL
                """,
                (
                    run_id,
                    "de_genesis_week5_nifi_downstream",
                    batch_id,
                    source_count,
                    raw_count,
                    source_count - raw_count,
                    raw_count,
                    accepted_count,
                    rejected_count,
                ),
            )
    return {
        "source_count": source_count,
        "inserted_count": raw_count,
        "duplicate_count": source_count - raw_count,
        "raw_count": raw_count,
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
    }


def run_shared_spark(source_mode: str) -> None:
    run_id, batch_id = context_ids(source_mode)
    summary = f"/workspace/output/week5/verification/{source_mode}-{batch_id}.json"
    command = [
        sys.executable,
        "-m",
        "exercises.week5.spark.transform_promotions",
        "--run-id",
        run_id,
        "--batch-id",
        batch_id,
        "--source-mode",
        source_mode,
        "--summary-path",
        summary,
    ]
    subprocess.run(command, check=True, env=os.environ.copy(), cwd="/workspace")


def extract_multisource() -> str:
    _, batch_id = context_ids("multisource")
    manifest = extract_multisource_batch(
        batch_id=batch_id,
        output_root="/workspace/output/week5/multisource",
        csv_path="/workspace/data/sample/sales.csv",
    )
    return str(manifest)


def run_multisource_spark() -> None:
    _, batch_id = context_ids("multisource")
    manifest = f"/workspace/output/week5/multisource/{batch_id}/manifest.json"
    command = [
        sys.executable,
        "-m",
        "exercises.week5.spark.multisource_report",
        "--manifest",
        manifest,
        "--output-root",
        "/workspace/output/week5/multisource",
    ]
    subprocess.run(command, check=True, env=os.environ.copy(), cwd="/workspace")


def failure_callback(context) -> None:
    dag_run = context.get("dag_run")
    if not dag_run:
        return
    exception = context.get("exception")
    try:
        mark_failure(dag_run.run_id, str(exception or "Task thất bại"))
    except Exception:
        # Callback không được che khuất lỗi gốc của task.
        pass
