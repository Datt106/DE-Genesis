from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from week5.tasks import check_dependencies, failure_callback, ingest_airflow, run_shared_spark


DEFAULT_ARGS = {
    "owner": "de-genesis",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": failure_callback,
}

with DAG(
    dag_id="de_genesis_week5_airflow_ingestion",
    description="Airflow gọi Promotion API, nạp raw và điều phối Spark core tuần 5",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["de-genesis", "week5", "airflow-centric"],
) as dag:
    start = EmptyOperator(task_id="start")
    dependencies = PythonOperator(task_id="check_dependencies", python_callable=check_dependencies)
    raw = PythonOperator(task_id="extract_and_load_raw", python_callable=ingest_airflow)
    transform = PythonOperator(
        task_id="run_shared_spark_transform",
        python_callable=run_shared_spark,
        op_kwargs={"source_mode": "airflow"},
    )
    end = EmptyOperator(task_id="end")

    start >> dependencies >> raw >> transform >> end
