from __future__ import annotations

import json
from pathlib import Path


FLOW_PATH = Path(__file__).resolve().parents[1] / "nifi" / "flow_definition.json"
NATIVE_FLOW_PATH = Path(__file__).resolve().parents[1] / "nifi" / "flow_definition_native.json"


def load_flow():
    return json.loads(FLOW_PATH.read_text(encoding="utf-8"))


def test_sensitive_parameters_have_no_value() -> None:
    flow = load_flow()
    context = flow["parameterContext"]
    for name in context["sensitiveParameters"]:
        assert context["parameters"][name] is None


def test_processor_ids_and_connections_are_consistent() -> None:
    flow = load_flow()
    processor_ids = {processor["id"] for processor in flow["processors"]}
    assert len(processor_ids) == len(flow["processors"])
    for source, _, destination in flow["connections"]:
        assert source in processor_ids
        assert destination in processor_ids


def test_airflow_trigger_is_idempotent() -> None:
    flow = load_flow()
    trigger = next(item for item in flow["processors"] if item["id"] == "build-airflow-trigger")
    value = trigger["properties"]["Replacement Value"]
    assert '"dag_run_id":"nifi__${batch_id}"' in value
    assert '"source_mode":"nifi"' in value


def test_required_failure_routes_are_documented() -> None:
    flow = load_flow()
    assert {"api-terminal-error", "invalid-record", "database-failure", "airflow-unauthorized"} <= set(
        flow["failureQueues"]
    )


def test_native_nifi_export_contains_expected_graph() -> None:
    native = json.loads(NATIVE_FLOW_PATH.read_text(encoding="utf-8"))
    contents = native["flowContents"]
    assert contents["name"] == "DE Genesis Week 5 - Promotion Ingestion"
    assert len(contents["processors"]) == 13
    assert len(contents["connections"]) == 14
