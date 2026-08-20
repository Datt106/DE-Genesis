from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_grafana_dashboard_is_valid_and_provisioned() -> None:
    dashboard_path = ROOT / "config" / "grafana" / "dashboards" / "de-genesis-pipeline.json"
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
    assert dashboard["uid"] == "de-genesis-pipeline"
    assert len(dashboard["panels"]) >= 10
    assert {panel["type"] for panel in dashboard["panels"]} >= {"stat", "timeseries"}
    provisioning = (
        ROOT / "config" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    ).read_text(encoding="utf-8")
    assert "uid: de-genesis-prometheus" in provisioning
    assert "url: http://prometheus:9090" in provisioning


def test_prometheus_scrapes_exporter_and_loads_alert_rules() -> None:
    prometheus = (ROOT / "config" / "prometheus" / "prometheus.yml").read_text(
        encoding="utf-8"
    )
    alerts = (
        ROOT / "config" / "prometheus" / "rules" / "de_genesis_alerts.yml"
    ).read_text(encoding="utf-8")
    assert "pipeline-metrics:9108" in prometheus
    assert "/etc/prometheus/rules/*.yml" in prometheus
    for alert in (
        "PipelineMetricsExporterDown",
        "PipelineMetricsCollectionError",
        "PipelineDependencyDown",
        "PipelineLastRunFailed",
        "PipelineDataQualityFailed",
        "PipelineStale",
        "PipelineRunStuck",
        "LogIngestionDelayExceeded",
        "LogStreamStopped",
        "LogStreamBatchFailed",
        "LogInvalidRecordsDetected",
        "LogReportRunFailed",
        "LogReportStale",
        "SparkWorkerUnavailable",
    ):
        assert f"alert: {alert}" in alerts
    assert 'pipeline_name!="de_genesis_week5_nifi_downstream"' in alerts


def test_metrics_exporter_exposes_pipeline_and_dependency_metrics() -> None:
    source = (
        ROOT / "exercises" / "week6" / "monitoring" / "exporter.py"
    ).read_text(encoding="utf-8")
    for metric in (
        "de_genesis_dependency_up",
        "de_genesis_metrics_collection_error",
        "de_genesis_pipeline_runs_total",
        "de_genesis_pipeline_last_success_timestamp_seconds",
        "de_genesis_pipeline_last_run_success",
        "de_genesis_pipeline_last_duration_seconds",
        "de_genesis_pipeline_last_rows",
        "de_genesis_pipeline_last_quality_failures",
        "de_genesis_pipeline_running",
        "de_genesis_log_ingestion_lag_seconds",
        "de_genesis_log_stream_last_batch_timestamp_seconds",
        "de_genesis_log_stream_last_batch_success",
        "de_genesis_log_stream_last_invalid_records",
        "de_genesis_log_report_last_run_success",
        "de_genesis_log_report_last_success_timestamp_seconds",
    ):
        assert metric in source


def test_dependency_health_requires_2xx_and_airflow_components() -> None:
    source = (
        ROOT / "exercises" / "week6" / "monitoring" / "exporter.py"
    ).read_text(encoding="utf-8")
    assert "200 <= response.status_code < 300" in source
    assert '("metadatabase", "scheduler")' in source
    assert 'payload[component].get("status") == "healthy"' in source


def test_running_run_is_not_reported_as_failed_terminal_run() -> None:
    source = (
        ROOT / "exercises" / "week6" / "monitoring" / "exporter.py"
    ).read_text(encoding="utf-8")
    assert "WHERE status NOT IN ('running','ingesting','transforming','publishing')" in source


def test_spark_runtime_matches_airflow_python_and_uses_cluster_by_default() -> None:
    dockerfile = (ROOT / ".docker" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    transform = (
        ROOT / "exercises" / "week6" / "spark" / "transform_promotions.py"
    ).read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "python:3.11-slim-bullseye" in dockerfile
    assert "PYSPARK_PYTHON=/usr/local/bin/python3.11" in dockerfile
    assert 'os.getenv("SPARK_MASTER_URL", "local[2]")' in transform
    assert "SPARK_DRIVER_HOST: airflow-scheduler" in compose
