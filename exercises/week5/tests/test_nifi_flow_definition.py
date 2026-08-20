from __future__ import annotations

import json
from pathlib import Path


FLOW_PATH = Path(__file__).resolve().parents[1] / "nifi" / "flow_definition.json"
NATIVE_FLOW_PATH = Path(__file__).resolve().parents[1] / "nifi" / "flow_definition_native.json"
NATIVE_METADATA_PATH = (
    Path(__file__).resolve().parents[1] / "nifi" / "flow_definition_native.metadata.json"
)
CONFIGURE_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "configure_nifi_flow.py"


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


def test_every_declared_relationship_is_connected_or_terminated() -> None:
    flow = load_flow()
    connected = {}
    for source, relationship, _ in flow["connections"]:
        connected.setdefault(source, set()).add(relationship)
    processors = {item["id"]: item for item in flow["processors"]}
    for processor_id, relationships in flow["relationshipContract"].items():
        accounted = connected.get(processor_id, set()) | set(
            processors[processor_id]["autoTerminatedRelationships"]
        )
        assert set(relationships) == accounted, processor_id


def test_pagination_reaches_all_250_records_before_trigger() -> None:
    flow = load_flow()
    by_id = {item["id"]: item for item in flow["processors"]}
    assert flow["parameterContext"]["parameters"]["api.page.size"] == "100"
    assert "pagination.has_next" in by_id["extract-pagination"]["properties"]
    assert ["route-page", "next_page", "increment-page"] in flow["connections"]
    assert ["increment-page", "success", "invoke-promotion-api"] in flow["connections"]
    merge = by_id["wait-batch-complete"]["properties"]
    assert merge["Merge Strategy"] == "Bin-Packing Algorithm"
    # MergeContent không hỗ trợ FlowFile Expression Language ở property này;
    # NiFi Parameter được resolve trước khi validate processor.
    assert merge["Minimum Number of Entries"] == "#{api.expected.records}"
    assert flow["parameterContext"]["parameters"]["api.expected.records"] == "250"
    trigger_value = by_id["build-airflow-trigger"]["properties"]["Replacement Value"]
    assert '"source_count":${pagination.total}' in trigger_value


def test_nifi_raw_upsert_is_scoped_by_batch() -> None:
    flow = load_flow()
    processor = next(item for item in flow["processors"] if item["id"] == "put-raw-postgres")
    assert processor["properties"]["Database Type"] == "PostgreSQL"
    assert processor["properties"]["Update Keys"].split(",")[0] == "batch_id"


def test_runtime_property_contracts_match_nifi_validators() -> None:
    flow = load_flow()
    by_id = {item["id"]: item for item in flow["processors"]}
    assert by_id["generate-batch"]["properties"]["Custom Text"] == "{}"
    assert by_id["route-page"]["properties"]["records"].endswith(":equals('true')}")
    assert by_id["hash-payload"]["properties"]["Hash Attribute Name"] == "payload_hash"
    assert set(by_id["hash-payload"]["removeProperties"]) == {
        "Attribute Name",
        "Fail When Empty",
    }


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
    assert contents["name"] == load_flow()["name"]
    assert native["flowEncodingVersion"] == "1.0"
    assert len(contents["processors"]) == 21
    assert len(contents["connections"]) == 46
    assert len(contents["controllerServices"]) == 2
    metadata = json.loads(NATIVE_METADATA_PATH.read_text(encoding="utf-8"))
    assert metadata["role"] == "runtime-export"
    assert metadata["snapshotVersion"] == "v2"
    assert metadata["doNotImport"] is False
    assert metadata["validatedWith"] == "Apache NiFi 1.27.0"
    assert metadata["processorCount"] == len(contents["processors"])
    assert metadata["connectionCount"] == len(contents["connections"])
    assert metadata["controllerServiceCount"] == len(contents["controllerServices"])
    assert metadata["canonicalBlueprint"] == "flow_definition.json"
    sensitive = {
        item["name"]: item
        for context in native["parameterContexts"].values()
        for item in context["parameters"]
        if item.get("sensitive")
    }
    assert {"airflow.password", "postgres.password"} <= sensitive.keys()
    assert all("value" not in item for item in sensitive.values())


def test_blueprint_versions_parameter_context_and_controller_services() -> None:
    flow = load_flow()
    assert flow["format"] == "de-genesis-nifi-flow/v2"
    assert flow["parameterContext"]["description"]
    assert {item["type"] for item in flow["controllerServices"]} == {
        "JsonTreeReader",
        "DBCPConnectionPool",
    }


def test_configure_script_automates_context_and_controller_services() -> None:
    source = CONFIGURE_SCRIPT.read_text(encoding="utf-8")
    assert "create_or_update_parameter_context" in source
    assert "assign_parameter_context" in source
    assert "clear_misnamed_processor_properties" in source
    assert "canonical_processor_properties" in source
    assert "canonical_controller_service_properties" in source
    assert "configure_controller_services" in source
    assert "wait_for_controller_service_state" in source
    assert "stop_group_for_configuration" in source
    assert '"id": context_id' in source
    assert "/controller-services/{service_id}/run-status" in source
    assert 'os.getenv("POSTGRES_PASSWORD")' in source
