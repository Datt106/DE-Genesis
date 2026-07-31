from __future__ import annotations

import argparse
import json
import os

import requests


def trigger(batch_id: str, *, base_url: str, username: str, password: str) -> dict:
    dag_run_id = f"nifi__{batch_id}"
    response = requests.post(
        f"{base_url.rstrip('/')}/api/v1/dags/de_genesis_week5_nifi_downstream/dagRuns",
        auth=(username, password),
        json={
            "dag_run_id": dag_run_id,
            "conf": {
                "source_mode": "nifi",
                "batch_id": batch_id,
                "raw_table": "week5_raw.promotions_nifi",
            },
        },
        timeout=10,
    )
    if response.status_code == 409:
        return {"status": "duplicate", "dag_run_id": dag_run_id}
    response.raise_for_status()
    return {"status": "triggered", "dag_run_id": dag_run_id, "response": response.json()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kiểm tra contract trigger NiFi -> Airflow")
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    result = trigger(
        args.batch_id,
        base_url=os.getenv("AIRFLOW_API_URL", "http://localhost:8088"),
        username=os.environ["AIRFLOW_API_USERNAME"],
        password=os.environ["AIRFLOW_API_PASSWORD"],
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
