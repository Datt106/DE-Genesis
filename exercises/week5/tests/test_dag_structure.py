from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DAGS = {
    "airflow": ROOT / "dags" / "de_genesis_week5_airflow_ingestion.py",
    "nifi": ROOT / "dags" / "de_genesis_week5_nifi_downstream.py",
    "multisource": ROOT / "dags" / "de_genesis_week5_multisource.py",
}


def test_dags_are_valid_python() -> None:
    for path in DAGS.values():
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_dags_disable_catchup_and_limit_active_runs() -> None:
    for path in DAGS.values():
        source = path.read_text(encoding="utf-8")
        assert "catchup=False" in source
        assert "max_active_runs=1" in source
        assert "execution_timeout" in source


def test_shared_spark_core_is_used() -> None:
    for path in (DAGS["airflow"], DAGS["nifi"]):
        assert "run_shared_spark" in path.read_text(encoding="utf-8")


def test_scheduled_dags_have_explicit_cron() -> None:
    airflow = DAGS["airflow"].read_text(encoding="utf-8")
    multisource = DAGS["multisource"].read_text(encoding="utf-8")
    assert 'schedule="0 6 * * *"' in airflow
    assert 'schedule="30 6 * * *"' in multisource
    # DAG NiFi downstream chỉ được kích hoạt từ REST, nên không có lịch riêng.
    assert "schedule=None" in DAGS["nifi"].read_text(encoding="utf-8")


def test_multisource_dag_covers_csv_postgres_rest_and_spark() -> None:
    source = DAGS["multisource"].read_text(encoding="utf-8")
    assert "extract_csv_postgres_rest" in source
    assert "run_multisource_spark" in source


def test_compose_passes_configured_api_page_size_to_python_runtimes() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    expected = "WEEK5_API_PAGE_SIZE: ${WEEK5_API_PAGE_SIZE:-100}"
    airflow_common = compose.split("x-airflow-common:", 1)[1].split("services:", 1)[0]
    workspace = compose.split("\n  workspace:", 1)[1].split("\n  postgres:", 1)[0]

    assert expected in airflow_common
    assert expected in workspace
