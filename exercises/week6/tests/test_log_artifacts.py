from __future__ import annotations

import pytest

from exercises.week6.log_artifacts import (
    ReportArtifactPaths,
    publish_staged_report,
    report_artifact_paths,
    validate_staged_report,
)


class FakeWebHdfsClient:
    def __init__(self, existing: set[str]) -> None:
        self.existing = set(existing)
        self.renames: list[tuple[str, str]] = []
        self.created: list[str] = []

    def exists(self, path: str) -> bool:
        return path in self.existing

    def mkdirs(self, path: str) -> None:
        self.created.append(path)
        self.existing.add(path)

    def rename(self, source: str, destination: str) -> None:
        self.renames.append((source, destination))
        moved = {
            path.replace(source, destination, 1)
            for path in self.existing
            if path == source or path.startswith(f"{source}/")
        }
        self.existing = {
            path
            for path in self.existing
            if path != source and not path.startswith(f"{source}/")
        }
        self.existing.update(moved)


def artifact_paths() -> ReportArtifactPaths:
    return report_artifact_paths(
        run_id="scheduled__2026-08-14T10:10:00+00:00",
        staging_base_uri="hdfs://namenode:9000/reports/_staging",
        published_base_uri="hdfs://namenode:9000/reports/closed",
    )


def staged_entries(paths: ReportArtifactPaths) -> set[str]:
    staging = paths.staging_uri.split(":9000", 1)[1]
    return {
        staging,
        f"{staging}/requests_per_minute/_SUCCESS",
        f"{staging}/status_distribution/_SUCCESS",
    }


def test_staging_quality_requires_both_success_markers() -> None:
    paths = artifact_paths()
    client = FakeWebHdfsClient(staged_entries(paths))
    assert validate_staged_report(client, paths.staging_uri)["artifacts"] == 2
    client.existing.remove(
        next(path for path in client.existing if "status_distribution/_SUCCESS" in path)
    )
    with pytest.raises(RuntimeError, match="chưa hoàn chỉnh"):
        validate_staged_report(client, paths.staging_uri)


def test_publish_renames_complete_staging_once_and_never_overwrites() -> None:
    paths = artifact_paths()
    client = FakeWebHdfsClient(staged_entries(paths))
    result = publish_staged_report(client, paths)
    assert result["mode"] == "atomic-rename"
    assert len(client.renames) == 1
    assert publish_staged_report(client, paths)["mode"] == "already-published"


def test_publish_blocks_when_staging_and_published_both_exist() -> None:
    paths = artifact_paths()
    published = paths.published_uri.split(":9000", 1)[1]
    client = FakeWebHdfsClient(staged_entries(paths) | {published})
    with pytest.raises(RuntimeError, match="đã tồn tại"):
        publish_staged_report(client, paths)
    assert client.renames == []


def test_retry_rejects_incomplete_published_artifact() -> None:
    paths = artifact_paths()
    published = paths.published_uri.split(":9000", 1)[1]
    client = FakeWebHdfsClient({published})
    with pytest.raises(RuntimeError, match="chưa hoàn chỉnh"):
        publish_staged_report(client, paths)
