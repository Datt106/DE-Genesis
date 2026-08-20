from __future__ import annotations

from pathlib import Path


WEEK1_DIR = Path(__file__).resolve().parents[1]


def read_sql(name: str) -> str:
    return (WEEK1_DIR / "script" / name).read_text(encoding="utf-8").lower()


def test_postgresql_lab_covers_procedure_trigger_and_plan() -> None:
    sql = read_sql("sql_practice_postgres.sql")

    assert "create or replace procedure" in sql
    assert "call olist_practice.refresh_week1_order_status_metrics()" in sql
    assert "create trigger trg_week1_order_status_audit" in sql
    assert "execute function olist_practice.audit_week1_order_status_change()" in sql
    assert "explain analyze" in sql


def test_postgresql_routines_do_not_depend_on_caller_search_path() -> None:
    sql = read_sql("sql_practice_postgres.sql")

    assert "procedure olist_practice.refresh_week1_order_status_metrics()" in sql
    assert "truncate table olist_practice.week1_order_status_metrics" in sql
    assert "insert into olist_practice.week1_order_status_metrics" in sql
    assert "from olist_practice.orders" in sql
    assert "function olist_practice.audit_week1_order_status_change()" in sql
    assert "insert into olist_practice.week1_order_status_audit" in sql
    assert "on olist_practice.week1_order_status_lab" in sql


def test_mysql_lab_covers_dialect_specific_objects() -> None:
    sql = read_sql("sql_practice_mysql.sql")

    assert "create procedure sp_week1_revenue_by_status" in sql
    assert "create trigger trg_week1_order_status_audit" in sql
    assert "on duplicate key update" in sql
    assert "delimiter $$" in sql
    assert "explain" in sql


def test_mysql_runtime_allows_application_user_to_create_lab_trigger() -> None:
    compose = (WEEK1_DIR.parents[1] / "docker-compose.yml").read_text(
        encoding="utf-8"
    )

    assert "--log-bin-trust-function-creators=1" in compose
