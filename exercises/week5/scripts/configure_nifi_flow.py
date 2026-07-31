from __future__ import annotations

import argparse
import json
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
        response.raise_for_status()
        self.session.headers["Authorization"] = f"Bearer {response.text}"

    def get(self, path: str) -> dict:
        response = self.session.get(f"{self.base_url}{path}", timeout=30)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, payload: dict) -> dict:
        response = self.session.post(f"{self.base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def put(self, path: str, payload: dict) -> dict:
        response = self.session.put(f"{self.base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def download(self, group_id: str, path: Path) -> None:
        response = self.session.get(
            f"{self.base_url}/process-groups/{group_id}/download",
            timeout=60,
        )
        response.raise_for_status()
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


def configure(blueprint: dict, client: NifiClient) -> dict:
    group_id = create_or_find_group(client, blueprint["name"])
    processor_catalog = type_catalog(client, "/flow/processor-types", "processorTypes")
    flow = client.get(f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"]
    processors = {item["component"]["name"]: item for item in flow.get("processors", [])}
    created_processors: dict[str, str] = {}
    warnings: list[str] = [
        "Hai Controller Service phải được tạo và gán Parameter Context trong NiFi UI để tránh đưa credential vào REST payload."
    ]

    for index, definition in enumerate(blueprint["processors"]):
        name = definition["name"]
        if name in processors:
            created_processors[definition["id"]] = processors[name]["id"]
            continue
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
        config: dict[str, Any] = {
            "properties": definition.get("properties", {}),
            "schedulingStrategy": definition.get("schedulingStrategy", "TIMER_DRIVEN"),
            "schedulingPeriod": definition.get("schedulingPeriod", "0 sec"),
            "autoTerminatedRelationships": definition.get("autoTerminatedRelationships", []),
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
    args = parser.parse_args()
    blueprint = json.loads(BLUEPRINT_PATH.read_text(encoding="utf-8"))
    result = configure(blueprint, NifiClient(args.url, args.username, args.password))
    print(json.dumps(result, ensure_ascii=False, indent=2))
