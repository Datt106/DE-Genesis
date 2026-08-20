"""DAG report/backfill cửa sổ đóng cho service log Tuần 6."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from week6.log_tasks import (
    check_log_dependencies,
    check_log_stream_health,
    initialize_log_audit,
    log_failure_callback,
    publish_log_report_task,
    quality_gate_log_report,
    resolve_log_configuration,
    run_spark_log_report,
)


DEFAULT_ARGS = {
    "owner": "de-genesis",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
    "sla": timedelta(minutes=6),
    "on_failure_callback": log_failure_callback,
}

with DAG(
    dag_id="de_genesis_week6_log_report",
    description="Report, backfill và health gate cho Kafka → Spark → HDFS/PostgreSQL",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 20, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["de-genesis", "week6", "logs", "streaming", "backfill"],
) as dag:
    start = EmptyOperator(task_id="start")
    resolve = PythonOperator(
        task_id="resolve_log_configuration",
        python_callable=resolve_log_configuration,
    )
    audit = PythonOperator(
        task_id="initialize_log_audit",
        python_callable=initialize_log_audit,
    )
    dependencies = PythonOperator(
        task_id="check_log_dependencies",
        python_callable=check_log_dependencies,
    )
    stream_health = PythonOperator(
        task_id="check_log_stream_health",
        python_callable=check_log_stream_health,
    )
    report = PythonOperator(
        task_id="run_spark_log_report",
        python_callable=run_spark_log_report,
    )
    quality = PythonOperator(
        task_id="quality_gate_log_report",
        python_callable=quality_gate_log_report,
        retries=0,
    )
    publish = PythonOperator(
        task_id="publish_log_report",
        python_callable=publish_log_report_task,
    )
    end = EmptyOperator(task_id="end")

    (
        start
        >> resolve
        >> audit
        >> dependencies
        >> stream_health
        >> report
        >> quality
        >> publish
        >> end
    )
