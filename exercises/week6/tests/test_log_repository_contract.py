from __future__ import annotations

from typing import Any

from exercises.week6 import log_repository


class FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.row: tuple[Any, ...] | None = None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, statement: str, _params=None) -> None:
        normalized = " ".join(statement.split())
        self.statements.append(normalized)
        if "COUNT(*) FILTER (WHERE check_status='passed')" in normalized:
            self.row = (8, 8, 0)
        elif "SELECT minute_report_count, status_report_count, status" in normalized:
            self.row = (2, 3, "success")
        else:
            raise AssertionError(f"Retry success không được chạy SQL mutation: {normalized}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self._cursor


class FakeDatabase:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def connect(self) -> FakeConnection:
        return FakeConnection(self._cursor)


class FakeDatabaseConfig:
    database: FakeDatabase | None = None

    @classmethod
    def from_env(cls) -> FakeDatabase:
        assert cls.database is not None
        return cls.database


def test_publish_retry_after_committed_success_is_a_database_noop(monkeypatch) -> None:
    cursor = FakeCursor()
    FakeDatabaseConfig.database = FakeDatabase(cursor)
    monkeypatch.setattr(log_repository, "DatabaseConfig", FakeDatabaseConfig)
    monkeypatch.setattr(log_repository, "ensure_schema", lambda _connection: None)

    result = log_repository.publish_log_report(
        {
            "run_id": "scheduled__already-success",
            "window_start": "2026-08-14T10:00:00+00:00",
            "window_end": "2026-08-14T10:05:00+00:00",
            "hdfs_staging_path": "hdfs://namenode:9000/_staging/run",
            "hdfs_published_path": "hdfs://namenode:9000/closed/run",
        }
    )

    assert result == {"minute_report_count": 2, "status_report_count": 3}
    assert not any(
        statement.startswith(("DELETE", "INSERT", "UPDATE"))
        for statement in cursor.statements
    )
