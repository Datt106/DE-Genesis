from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DAGS = {
    "airflow": ROOT / "dags" / "de_genesis_week5_airflow_ingestion.py",
    "nifi": ROOT / "dags" / "de_genesis_week5_nifi_downstream.py",
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
    for path in DAGS.values():
        assert "run_shared_spark" in path.read_text(encoding="utf-8")
