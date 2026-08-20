from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
STREAM = ROOT / "exercises" / "week6" / "spark" / "stream_service_logs.py"
REPORT = ROOT / "exercises" / "week6" / "spark" / "report_service_logs.py"
DAG = ROOT / "dags" / "de_genesis_week6_log_report.py"
DDL = ROOT / "exercises" / "week6" / "sql" / "create_week6_schemas.sql"
EXPORTER = ROOT / "exercises" / "week6" / "monitoring" / "exporter.py"
ALERTS = ROOT / "config" / "prometheus" / "rules" / "de_genesis_alerts.yml"
COMPOSE = ROOT / "docker-compose.yml"
GENERATION_MIGRATION_TEST = (
    ROOT / "exercises" / "week6" / "sql" / "test_log_generation_migration.sql"
)


def test_new_log_assets_are_valid_python() -> None:
    for path in (STREAM, REPORT, DAG):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_stream_enforces_kafka_hdfs_rotation_and_sla_contracts() -> None:
    source = STREAM.read_text(encoding="utf-8")
    for expected in (
        'spark.readStream.format("kafka")',
        'option("failOnDataLoss", "true")',
        "withWatermark(",
        ".foreachBatch(",
        "validate_micro_batch_seconds",
        'os.getenv("WEEK6_LOG_STREAM_MAX_CORES", "1")',
        'os.getenv("HDFS_REPLICATION", "1")',
        "publish_stream_batch",
        "week6_log.requests_per_minute_stream_staging",
        "week6_log.status_distribution_stream_staging",
    ):
        assert expected in source
    partition_start = source.index(".partitionBy(")
    partition_end = source.index(").parquet", partition_start)
    partition_block = source[partition_start:partition_end]
    hierarchy = [
        partition_block.index(f'"{column}"')
        for column in (
            "ingest_date",
            "ingest_hour",
            "rotation_5m",
            "stream_generation_id",
            "stream_batch_id",
        )
    ]
    assert hierarchy == sorted(hierarchy)
    compact = "".join(source.split())
    assert (
        '.partitionBy("ingest_date","ingest_hour","rotation_5m",'
        '"stream_generation_id","stream_batch_id")'
        in compact
    )
    assert "spark.sql.sources.partitionOverwriteMode" in source
    assert 'F.trim("service")' in source
    assert 'F.trim("path")' in source


def test_report_uses_distributed_jdbc_and_no_driver_row_materialization() -> None:
    combined = STREAM.read_text(encoding="utf-8") + REPORT.read_text(encoding="utf-8")
    assert 'os.getenv("WEEK6_LOG_REPORT_MAX_CORES", "1")' in combined
    assert combined.count('os.getenv("HDFS_REPLICATION", "1")') == 2
    assert ".write.mode(\"append\").jdbc(" in combined
    assert "toLocalIterator" not in combined
    assert "fetchall()" not in combined
    assert "stream_generation_id=*/stream_batch_id=*" in combined
    assert "rotation_5m=*/stream_batch_id=*" in combined
    assert "stream_batch_id=*/ingest_date=*" in combined
    assert '.dropDuplicates(["event_id"])' in combined
    assert "--report-staging-path" in combined


def test_log_dag_orders_health_report_quality_and_publish() -> None:
    source = DAG.read_text(encoding="utf-8")
    expected_order = [
        'task_id="resolve_log_configuration"',
        'task_id="initialize_log_audit"',
        'task_id="check_log_dependencies"',
        'task_id="check_log_stream_health"',
        'task_id="run_spark_log_report"',
        'task_id="quality_gate_log_report"',
        'task_id="publish_log_report"',
    ]
    positions = [source.index(task) for task in expected_order]
    assert positions == sorted(positions)
    assert 'schedule="*/5 * * * *"' in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source


def test_ddl_has_telemetry_staging_live_and_canonical_contracts() -> None:
    ddl = DDL.read_text(encoding="utf-8")
    for relation in (
        "week6_control.log_stream_batches",
        "week6_control.log_stream_generations",
        "week6_control.log_report_runs",
        "week6_control.log_quality_results",
        "week6_log.requests_per_minute_stream_staging",
        "week6_log.status_distribution_stream_staging",
        "week6_log.live_requests_per_minute",
        "week6_log.live_status_distribution",
        "week6_log.requests_per_minute_staging",
        "week6_log.status_distribution_staging",
        "week6_log.requests_per_minute",
        "week6_log.status_distribution",
    ):
        assert relation in ddl
    assert "PRIMARY KEY (stream_generation_id, stream_batch_id)" in ddl


def test_checkpoint_reset_and_quality_publish_are_blocking_contracts() -> None:
    repository = (
        ROOT / "exercises" / "week6" / "log_repository.py"
    ).read_text(encoding="utf-8")
    contracts = (
        ROOT / "exercises" / "week6" / "log_contracts.py"
    ).read_text(encoding="utf-8")
    tasks = (ROOT / "dags" / "week6" / "log_tasks.py").read_text(
        encoding="utf-8"
    )
    assert "stream_batch_id < last_successful_batch_id" in contracts
    assert "WEEK6_LOG_GENERATION_ID" in repository
    assert "EXPECTED_LOG_QUALITY_CHECKS = 8" in repository
    assert "assert_log_report_quality_ready" in tasks
    assert tasks.index("artifact_result = validate_staged_report") < tasks.index(
        "artifact_result = publish_staged_report"
    )
    assert 'if run_status == "success"' in repository
    assert "status <> 'success'" in repository
    assert "lineage_id" in repository
    migration_test = GENERATION_MIGRATION_TEST.read_text(encoding="utf-8")
    assert "legacy_high_water <> 7" in migration_test
    assert migration_test.count("\\ir create_week6_schemas.sql") == 2


def test_stream_health_uses_latest_terminal_and_detects_stuck_running_batch() -> None:
    source = (
        ROOT / "exercises" / "week6" / "log_repository.py"
    ).read_text(encoding="utf-8")
    assert "WHERE finished_at IS NOT NULL" in source
    assert "status='running'" in source
    assert "max_running_seconds" in source


def test_cold_start_metrics_and_alerts_never_depend_on_existing_rows() -> None:
    exporter = EXPORTER.read_text(encoding="utf-8")
    alerts = ALERTS.read_text(encoding="utf-8")
    assert '"de_genesis_week6_service_logs"' in exporter
    assert "stream_values = (expected_query_name, 0.0, 0.0, 0, 0)" in exporter
    assert "latest_success_timestamp = 0.0" in exporter
    assert "de_genesis_log_report_last_run_timestamp_seconds" in exporter
    assert "WHERE status IN ('success','failed')" in exporter
    assert "absent(de_genesis_log_stream_last_batch_timestamp_seconds)" in alerts
    assert "absent(de_genesis_log_report_last_success_timestamp_seconds)" in alerts
    assert (
        "de_genesis_log_report_last_run_timestamp_seconds > 0" in alerts
    )


def test_airflow_receives_same_settlement_and_report_resource_contract() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    airflow_environment = compose.split("depends_on:", 1)[0]
    for variable in (
        "WEEK6_LOG_MAX_EVENT_DELAY_SECONDS",
        "WEEK6_LOG_GENERATION_ID",
        "WEEK6_LOG_REPORT_SETTLEMENT_SECONDS",
        "WEEK6_LOG_REPORT_MAX_CORES",
        "WEEK6_LOG_REPORT_STAGING_PATH",
        "WEEK6_NAMENODE_WEBHDFS_URL",
    ):
        assert variable in airflow_environment
