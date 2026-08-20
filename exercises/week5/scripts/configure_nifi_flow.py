from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import requests
import urllib3


HERE = Path(__file__).resolve().parent
BLUEPRINT_PATH = HERE.parent / "nifi" / "flow_definition.json"
NATIVE_EXPORT_PATH = HERE.parent / "nifi" / "flow_definition_native.json"


class NifiClient:
    def __init__(self, base_url: str, username: str, password: str):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.base_url = base_url.rstrip("/") + "/nifi-api"
        self.session = requests.Session()
        self.session.verify = False
        response = self.session.post(
            f"{self.base_url}/access/token",
            data={"username": username, "password": password},
            timeout=20,
        )
        self.ensure_success(response)
        self.session.headers["Authorization"] = f"Bearer {response.text}"

    @staticmethod
    def ensure_success(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            body = response.text.strip().replace("\n", " ")[:2000]
            raise requests.HTTPError(
                f"NiFi REST {response.request.method} {response.url} trả "
                f"HTTP {response.status_code}: {body}",
                response=response,
                request=response.request,
            ) from error

    def get(self, path: str) -> dict:
        response = self.session.get(f"{self.base_url}{path}", timeout=30)
        self.ensure_success(response)
        return response.json()

    def post(self, path: str, payload: dict) -> dict:
        response = self.session.post(f"{self.base_url}{path}", json=payload, timeout=30)
        self.ensure_success(response)
        return response.json()

    def put(self, path: str, payload: dict) -> dict:
        response = self.session.put(f"{self.base_url}{path}", json=payload, timeout=30)
        self.ensure_success(response)
        return response.json()

    def download(self, group_id: str, path: Path) -> None:
        response = self.session.get(
            f"{self.base_url}/process-groups/{group_id}/download",
            timeout=60,
        )
        self.ensure_success(response)
        path.write_bytes(response.content)


def type_catalog(client: NifiClient, endpoint: str, key: str) -> dict[str, dict]:
    result = {}
    for item in client.get(endpoint).get(key, []):
        simple = item["type"].rsplit(".", 1)[-1]
        result[simple] = item
    return result


def create_or_find_group(client: NifiClient, name: str) -> str:
    root = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    flow = client.get(f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]
    existing = next((group for group in flow.get("processGroups", []) if group["component"]["name"] == name), None)
    if existing:
        return existing["id"]
    entity = client.post(
        f"/process-groups/{root}/process-groups",
        {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "position": {"x": 100.0, "y": 100.0},
                "parentGroupId": root,
            },
        },
    )
    return entity["id"]


def stop_group_for_configuration(client: NifiClient, group_id: str) -> None:
    """Đưa processor/service về trạng thái cho phép cập nhật cấu hình."""

    client.put(
        f"/flow/process-groups/{group_id}",
        {
            "id": group_id,
            "state": "STOPPED",
            "disconnectedNodeAcknowledged": False,
        },
    )
    client.put(
        f"/flow/process-groups/{group_id}/controller-services",
        {
            "id": group_id,
            "state": "DISABLED",
            "disconnectedNodeAcknowledged": False,
        },
    )
    services = client.get(
        f"/flow/process-groups/{group_id}/controller-services"
    ).get("controllerServices", [])
    wait_for_controller_service_state(
        client,
        [service["id"] for service in services],
        "DISABLED",
    )


def wait_for_controller_service_state(
    client: NifiClient,
    service_ids: list[str],
    expected_state: str,
    timeout_seconds: float = 30.0,
) -> None:
    """Đợi chuyển trạng thái bất đồng bộ của Controller Service có giới hạn."""

    if not service_ids:
        return
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        states = {
            service_id: client.get(f"/controller-services/{service_id}")
            ["component"]["state"]
            for service_id in service_ids
        }
        if all(state == expected_state for state in states.values()):
            return
        time.sleep(0.5)
    raise TimeoutError(
        f"Controller Service không về trạng thái {expected_state} trong "
        f"{timeout_seconds:.0f} giây"
    )


def parameter_payload(definition: dict, secrets: dict[str, str | None]) -> list[dict]:
    sensitive = set(definition.get("sensitiveParameters", []))
    parameters = []
    for name, configured_value in definition["parameters"].items():
        value = secrets.get(name) if name in sensitive else configured_value
        if name in sensitive and not value:
            raise ValueError(f"Thiếu secret runtime cho NiFi parameter: {name}")
        parameters.append(
            {
                "parameter": {
                    "name": name,
                    "description": f"DE Genesis Week 5: {name}",
                    "sensitive": name in sensitive,
                    "value": value,
                }
            }
        )
    return parameters


def create_or_update_parameter_context(
    client: NifiClient,
    definition: dict,
    secrets: dict[str, str | None],
) -> str:
    entities = client.get("/flow/parameter-contexts").get("parameterContexts", [])
    existing = next(
        (
            entity
            for entity in entities
            if entity.get("component", {}).get("name") == definition["name"]
        ),
        None,
    )
    component = {
        "name": definition["name"],
        "description": definition.get("description", ""),
        "parameters": parameter_payload(definition, secrets),
    }
    if existing:
        context_id = existing["id"]
        component["id"] = context_id
        entity = client.put(
            f"/parameter-contexts/{context_id}",
            {
                "id": context_id,
                "revision": existing["revision"],
                "component": component,
            },
        )
    else:
        entity = client.post(
            "/parameter-contexts",
            {"revision": {"version": 0}, "component": component},
        )
    return entity["id"]


def assign_parameter_context(client: NifiClient, group_id: str, context_id: str) -> None:
    group = client.get(f"/process-groups/{group_id}")
    client.put(
        f"/process-groups/{group_id}",
        {
            "id": group_id,
            "revision": group["revision"],
            "component": {
                "id": group_id,
                "parameterContext": {"id": context_id},
            },
        },
    )


def processor_property_aliases(entity: dict) -> dict[str, str]:
    """Ánh xạ display name sang API name; NiFi REST chỉ nhận API name chuẩn."""

    descriptors = entity.get("component", {}).get("config", {}).get(
        "descriptors", {}
    )
    return {
        descriptor.get("displayName", api_name): api_name
        for api_name, descriptor in descriptors.items()
        if not descriptor.get("dynamic", False)
    }


def clear_misnamed_processor_properties(
    client: NifiClient,
    flow: dict,
    warnings: list[str],
) -> None:
    """Dọn dynamic property sinh nhầm từ display name trước khi gán context."""

    for entity in flow.get("processors", []):
        config = entity.get("component", {}).get("config", {})
        descriptors = config.get("descriptors", {})
        aliases = processor_property_aliases(entity)
        stale = {
            name: None
            for name, value in config.get("properties", {}).items()
            if value is not None
            and (
                name not in descriptors
                or descriptors[name].get("dynamic", False)
            )
            and aliases.get(name, name) != name
        }
        if not stale:
            continue
        try:
            client.put(
                f"/processors/{entity['id']}",
                {
                    "revision": entity["revision"],
                    "component": {
                        "id": entity["id"],
                        "config": {"properties": stale},
                    },
                },
            )
        except requests.HTTPError as error:
            warnings.append(
                f"Không dọn được property alias của "
                f"{entity['component']['name']}: {error}"
            )


def canonical_processor_properties(
    entity: dict,
    configured: dict[str, Any],
    service_ids: dict[str, str],
) -> dict[str, Any]:
    """Chuẩn hóa property blueprint từ display name về API name của runtime."""

    descriptors = entity.get("component", {}).get("config", {}).get(
        "descriptors", {}
    )
    aliases = processor_property_aliases(entity)
    resolved: dict[str, Any] = {}
    for name, value in configured.items():
        descriptor = descriptors.get(name, {})
        api_name = (
            name
            if name in descriptors and not descriptor.get("dynamic", False)
            else aliases.get(name, name)
        )
        canonical_value = service_ids.get(value, value)
        api_descriptor = descriptors.get(api_name, {})
        if isinstance(canonical_value, str):
            for option in api_descriptor.get("allowableValues", []):
                allowed = option.get("allowableValue", {})
                if str(allowed.get("displayName", "")).casefold() == canonical_value.casefold():
                    canonical_value = allowed.get("value", canonical_value)
                    break
        resolved[api_name] = canonical_value
        if api_name != name:
            resolved[name] = None
    return resolved


def canonical_controller_service_properties(
    entity: dict,
    configured: dict[str, Any],
) -> dict[str, Any]:
    """Chuẩn hóa property Controller Service theo descriptor của NiFi runtime.

    NiFi cho phép dynamic property. Nếu gửi display name thay vì API name,
    REST API sẽ im lặng tạo một dynamic property trùng tên và làm service
    INVALID. Hàm này đồng thời xóa alias sai đã tồn tại từ lần
    cấu hình trước.
    """

    descriptors = entity.get("component", {}).get("descriptors", {})
    aliases = {
        descriptor.get("displayName", api_name): api_name
        for api_name, descriptor in descriptors.items()
        if not descriptor.get("dynamic", False)
    }
    resolved: dict[str, Any] = {}
    for name, value in configured.items():
        descriptor = descriptors.get(name, {})
        api_name = (
            name
            if name in descriptors and not descriptor.get("dynamic", False)
            else aliases.get(name, name)
        )
        canonical_value = value
        api_descriptor = descriptors.get(api_name, {})
        if isinstance(canonical_value, str):
            for option in api_descriptor.get("allowableValues", []):
                allowed = option.get("allowableValue", {})
                if (
                    str(allowed.get("displayName", "")).casefold()
                    == canonical_value.casefold()
                ):
                    canonical_value = allowed.get("value", canonical_value)
                    break
        resolved[api_name] = canonical_value
        if api_name != name:
            resolved[name] = None

    for name, value in entity.get("component", {}).get("properties", {}).items():
        if (
            value is not None
            and descriptors.get(name, {}).get("dynamic", False)
            and aliases.get(name, name) != name
        ):
            resolved[name] = None
    return resolved


def configure_controller_services(
    client: NifiClient,
    group_id: str,
    definitions: list[dict],
    warnings: list[str],
) -> dict[str, str]:
    catalog = type_catalog(
        client,
        "/flow/controller-service-types",
        "controllerServiceTypes",
    )
    existing = {
        item["component"]["name"]: item
        for item in client.get(
            f"/flow/process-groups/{group_id}/controller-services"
        ).get("controllerServices", [])
    }
    service_ids: dict[str, str] = {}
    enabled_requested: list[str] = []
    for definition in definitions:
        name = definition.get("name", definition["id"])
        entity = existing.get(name)
        if entity is None:
            type_info = catalog.get(definition["type"])
            if not type_info:
                warnings.append(f"Không tìm thấy Controller Service: {definition['type']}")
                continue
            entity = client.post(
                f"/process-groups/{group_id}/controller-services",
                {
                    "revision": {"version": 0},
                    "component": {
                        "name": name,
                        "type": type_info["type"],
                        "bundle": type_info["bundle"],
                    },
                },
            )
        service_id = entity["id"]
        service_ids[definition["id"]] = service_id
        try:
            entity = client.get(f"/controller-services/{service_id}")
            updated = client.put(
                f"/controller-services/{service_id}",
                {
                    "revision": entity["revision"],
                    "component": {
                        "id": service_id,
                        "name": name,
                        "properties": canonical_controller_service_properties(
                            entity,
                            definition.get("properties", {}),
                        ),
                    },
                },
            )
            client.put(
                f"/controller-services/{service_id}/run-status",
                {
                    "revision": updated["revision"],
                    "state": "ENABLED",
                    "disconnectedNodeAcknowledged": False,
                },
            )
            enabled_requested.append(service_id)
        except requests.HTTPError as error:
            warnings.append(f"Không bật được Controller Service {name}: {error}")
    try:
        wait_for_controller_service_state(
            client,
            enabled_requested,
            "ENABLED",
        )
    except TimeoutError as error:
        warnings.append(str(error))
    return service_ids


def configure(
    blueprint: dict,
    client: NifiClient,
    secrets: dict[str, str | None],
) -> dict:
    group_id = create_or_find_group(client, blueprint["name"])
    stop_group_for_configuration(client, group_id)
    warnings: list[str] = []
    parameter_context_id = create_or_update_parameter_context(
        client,
        blueprint["parameterContext"],
        secrets,
    )
    flow = client.get(f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"]
    clear_misnamed_processor_properties(client, flow, warnings)
    assign_parameter_context(client, group_id, parameter_context_id)
    processor_catalog = type_catalog(client, "/flow/processor-types", "processorTypes")
    flow = client.get(f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"]
    processors = {item["component"]["name"]: item for item in flow.get("processors", [])}
    created_processors: dict[str, str] = {}
    service_ids = configure_controller_services(
        client,
        group_id,
        blueprint.get("controllerServices", []),
        warnings,
    )

    for index, definition in enumerate(blueprint["processors"]):
        name = definition["name"]
        if name in processors:
            entity = processors[name]
        else:
            type_info = processor_catalog.get(definition["type"])
            if not type_info:
                warnings.append(f"Không tìm thấy processor type: {definition['type']}")
                continue
            entity = client.post(
                f"/process-groups/{group_id}/processors",
                {
                    "revision": {"version": 0},
                    "component": {
                        "name": name,
                        "type": type_info["type"],
                        "bundle": type_info["bundle"],
                        "position": {
                            "x": float(200 + (index % 4) * 380),
                            "y": float(150 + (index // 4) * 240),
                        },
                    },
                },
            )
        created_processors[definition["id"]] = entity["id"]
        properties = canonical_processor_properties(
            entity,
            definition.get("properties", {}),
            service_ids,
        )
        properties.update(
            {name: None for name in definition.get("removeProperties", [])}
        )
        config: dict[str, Any] = {
            "properties": properties,
            "schedulingStrategy": definition.get("schedulingStrategy", "TIMER_DRIVEN"),
            "schedulingPeriod": definition.get("schedulingPeriod", "0 sec"),
            "autoTerminatedRelationships": definition.get("autoTerminatedRelationships", []),
            "retryCount": definition.get("maxRetries", 0),
        }
        try:
            client.put(
                f"/processors/{entity['id']}",
                {
                    "revision": entity["revision"],
                    "component": {"id": entity["id"], "config": config},
                },
            )
        except requests.HTTPError as error:
            warnings.append(f"Không áp dụng đủ property cho {name}: {error}")

    refreshed = client.get(f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"]
    existing_connections = {
        (
            item["component"]["source"]["id"],
            item["component"]["destination"]["id"],
            tuple(item["component"].get("selectedRelationships", [])),
        )
        for item in refreshed.get("connections", [])
    }
    for source_key, relationship, destination_key in blueprint.get("connections", []):
        source_id = created_processors.get(source_key)
        destination_id = created_processors.get(destination_key)
        if not source_id or not destination_id:
            continue
        source_entity = client.get(f"/processors/{source_id}")
        available = {
            item["name"].lower(): item["name"]
            for item in source_entity["component"].get("relationships", [])
        }
        selected = available.get(relationship.lower())
        if not selected:
            warnings.append(
                f"Relationship '{relationship}' không tồn tại trên processor {source_key}"
            )
            continue
        identity = (source_id, destination_id, (selected,))
        if identity in existing_connections:
            continue
        try:
            client.post(
                f"/process-groups/{group_id}/connections",
                {
                    "revision": {"version": 0},
                    "component": {
                        "name": f"{source_key}__{selected}__{destination_key}",
                        "source": {
                            "id": source_id,
                            "groupId": group_id,
                            "type": "PROCESSOR",
                        },
                        "destination": {
                            "id": destination_id,
                            "groupId": group_id,
                            "type": "PROCESSOR",
                        },
                        "selectedRelationships": [selected],
                        "flowFileExpiration": "0 sec",
                        "backPressureObjectThreshold": 10000,
                        "backPressureDataSizeThreshold": "1 GB",
                    },
                },
            )
        except requests.HTTPError as error:
            warnings.append(
                f"Không tạo được connection {source_key}->{destination_key}: {error}"
            )

    client.download(group_id, NATIVE_EXPORT_PATH)
    return {
        "group_id": group_id,
        "parameter_context_id": parameter_context_id,
        "controller_service_count": len(service_ids),
        "processor_count": len(created_processors),
        "connection_count": len(
            client.get(f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"].get(
                "connections", []
            )
        ),
        "native_export": str(NATIVE_EXPORT_PATH),
        "warnings": warnings,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tạo Process Group tuần 5 qua NiFi REST API")
    parser.add_argument("--url", default="https://localhost:8443")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", required=True)
    parser.add_argument("--airflow-password", default=os.getenv("AIRFLOW_ADMIN_PASSWORD"))
    parser.add_argument("--postgres-password", default=os.getenv("POSTGRES_PASSWORD"))
    args = parser.parse_args()
    blueprint = json.loads(BLUEPRINT_PATH.read_text(encoding="utf-8"))
    result = configure(
        blueprint,
        NifiClient(args.url, args.username, args.password),
        {
            "airflow.password": args.airflow_password,
            "postgres.password": args.postgres_password,
        },
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
