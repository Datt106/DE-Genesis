from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from week5.tasks import (
    check_dependencies,
    extract_multisource,
    failure_callback,
    run_multisource_spark,
)


DEFAULT_ARGS = {
    "owner": "de-genesis",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": failure_callback,
}

with DAG(
    dag_id="de_genesis_week5_multisource",
    description="CSV + PostgreSQL + REST API qua Spark, lưu báo cáo Parquet",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 1),
    schedule="30 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["de-genesis", "week5", "multisource"],
) as dag:
    start = EmptyOperator(task_id="start")
    dependencies = PythonOperator(task_id="check_dependencies", python_callable=check_dependencies)
    extract = PythonOperator(task_id="extract_csv_postgres_rest", python_callable=extract_multisource)
    transform = PythonOperator(task_id="spark_build_reports", python_callable=run_multisource_spark)
    end = EmptyOperator(task_id="end")

    start >> dependencies >> extract >> transform >> end
