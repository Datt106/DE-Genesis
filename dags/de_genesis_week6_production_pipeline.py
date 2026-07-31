from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from week6.tasks import (
    check_dependencies,
    failure_callback,
    finish_run,
    ingest_incremental,
    initialize_audit,
    quality_gate_curated,
    quality_gate_raw,
    resolve_configuration,
    run_spark_snapshot,
)


DEFAULT_ARGS = {
    "owner": "de-genesis",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
    "sla": timedelta(hours=1),
    "on_failure_callback": failure_callback,
}

with DAG(
    dag_id="de_genesis_week6_production_pipeline",
    description="Promotion pipeline production có incremental window, audit, DQ gate và monitoring",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 20, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["de-genesis", "week6", "production", "monitoring"],
) as dag:
    start = EmptyOperator(task_id="start")
    resolve = PythonOperator(
        task_id="resolve_configuration",
        python_callable=resolve_configuration,
    )
    audit = PythonOperator(task_id="initialize_audit", python_callable=initialize_audit)
    dependencies = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_dependencies,
    )
    ingestion = PythonOperator(
        task_id="ingest_incremental",
        python_callable=ingest_incremental,
    )
    raw_gate = PythonOperator(
        task_id="quality_gate_raw",
        python_callable=quality_gate_raw,
        retries=0,
    )
    transform = PythonOperator(
        task_id="run_spark_snapshot",
        python_callable=run_spark_snapshot,
    )
    curated_gate = PythonOperator(
        task_id="quality_gate_curated",
        python_callable=quality_gate_curated,
        retries=0,
    )
    finalize = PythonOperator(task_id="finalize_success", python_callable=finish_run)
    end = EmptyOperator(task_id="end")

    (
        start
        >> resolve
        >> audit
        >> dependencies
        >> ingestion
        >> raw_gate
        >> transform
        >> curated_gate
        >> finalize
        >> end
    )
