from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DAG_PATH = ROOT / "dags" / "de_genesis_week6_production_pipeline.py"


def test_dag_is_valid_python() -> None:
    ast.parse(DAG_PATH.read_text(encoding="utf-8"), filename=str(DAG_PATH))


def test_dag_has_production_guards() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    for expected in (
        'schedule="0 2 * * *"',
        "catchup=False",
        "max_active_runs=1",
        '"retry_exponential_backoff": True',
        '"execution_timeout"',
        '"sla"',
        "on_failure_callback",
    ):
        assert expected in source


def test_dag_orders_audit_quality_and_transform_tasks() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    expected_order = [
        'task_id="resolve_configuration"',
        'task_id="initialize_audit"',
        'task_id="check_dependencies"',
        'task_id="ingest_incremental"',
        'task_id="quality_gate_raw"',
        'task_id="run_spark_snapshot"',
        'task_id="quality_gate_curated"',
        'task_id="publish_curated_snapshot"',
        'task_id="finalize_success"',
    ]
    positions = [source.index(task) for task in expected_order]
    assert positions == sorted(positions)
    assert source.count("retries=0") == 2
