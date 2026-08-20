from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_raw_uniqueness_includes_batch_and_run_audit_has_reconciliation_counts() -> None:
    ddl = (ROOT / "sql" / "create_week5_schemas.sql").read_text(encoding="utf-8")
    normalized_ddl = " ".join(ddl.split())
    assert "batch_id, source_system, promotion_id, payload_hash" in normalized_ddl
    assert "NULLS NOT DISTINCT" in ddl
    assert "indnullsnotdistinct" in ddl
    assert "ROW_NUMBER() OVER" in ddl
    assert "pg_advisory_xact_lock" in ddl
    regression_sql = (ROOT / "sql" / "test_null_idempotency.sql").read_text(
        encoding="utf-8"
    )
    assert "promotion_id, product_id" in regression_sql
    assert "NULL, NULL" in regression_sql
    assert "ROLLBACK;" in regression_sql
    assert "source_count BIGINT" in ddl
    assert "inserted_count BIGINT" in ddl
    assert "duplicate_count BIGINT" in ddl
    assert "source_count = inserted_count + duplicate_count" in ddl
    assert "raw_count = accepted_count + rejected_count" in ddl


def test_airflow_ingestion_counts_rows_actually_stored() -> None:
    source = (ROOT / "ingestion.py").read_text(encoding="utf-8")
    assert "inserted_count += cursor.rowcount" in source
    assert "ON CONFLICT (\n                        batch_id, source_system, promotion_id, payload_hash" in source
    assert ") DO NOTHING" in source
    assert "FROM week5_raw.promotions_airflow" in source


def test_existing_transform_uses_compose_spark_master() -> None:
    source = (ROOT / "spark" / "transform_promotions.py").read_text(encoding="utf-8")
    assert 'os.getenv("WEEK5_SPARK_MASTER")' in source
    assert 'os.getenv("SPARK_MASTER_URL")' in source
