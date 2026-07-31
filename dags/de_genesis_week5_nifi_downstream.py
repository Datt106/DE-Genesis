from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from week5.tasks import failure_callback, prepare_nifi_run, run_shared_spark


DEFAULT_ARGS = {
    "owner": "de-genesis",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": failure_callback,
}

with DAG(
    dag_id="de_genesis_week5_nifi_downstream",
    description="DAG downstream được NiFi kích hoạt qua Airflow REST API",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["de-genesis", "week5", "nifi-centric"],
) as dag:
    start = EmptyOperator(task_id="start")
    validate = PythonOperator(task_id="validate_trigger_and_raw_batch", python_callable=prepare_nifi_run)
    transform = PythonOperator(
        task_id="run_shared_spark_transform",
        python_callable=run_shared_spark,
        op_kwargs={"source_mode": "nifi"},
    )
    end = EmptyOperator(task_id="end")

    start >> validate >> transform >> end
