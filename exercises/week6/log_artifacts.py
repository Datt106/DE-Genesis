"""Hợp đồng staging và atomic rename cho report file trên HDFS."""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import quote, urlparse

import requests


@dataclass(frozen=True)
class ReportArtifactPaths:
    staging_uri: str
    published_uri: str


def safe_run_path_segment(run_id: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", run_id).strip("._-")[:160]
    if not value:
        raise ValueError("run_id không tạo được HDFS path an toàn")
    return value


def report_artifact_paths(
    *,
    run_id: str,
    staging_base_uri: str,
    published_base_uri: str,
) -> ReportArtifactPaths:
    segment = safe_run_path_segment(run_id)
    return ReportArtifactPaths(
        staging_uri=f"{staging_base_uri.rstrip('/')}/run_id={segment}",
        published_uri=f"{published_base_uri.rstrip('/')}/run_id={segment}",
    )


def hdfs_path(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "hdfs" or not parsed.path.startswith("/"):
        raise ValueError(f"Artifact path phải là URI hdfs:// hợp lệ: {uri}")
    return parsed.path


class WebHdfsClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 10,
        user_name: str = "root",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.user_name = user_name

    def _url(self, path: str, operation: str, **params: str) -> str:
        query = "&".join(
            [f"op={quote(operation)}", f"user.name={quote(self.user_name)}"]
            + [f"{quote(key)}={quote(str(value), safe='/=:._-')}" for key, value in params.items()]
        )
        return f"{self.base_url}/webhdfs/v1{quote(path, safe='/=._-')}?{query}"

    def exists(self, path: str) -> bool:
        response = requests.get(
            self._url(path, "GETFILESTATUS"), timeout=self.timeout_seconds
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def mkdirs(self, path: str) -> None:
        response = requests.put(
            self._url(path, "MKDIRS"), timeout=self.timeout_seconds
        )
        response.raise_for_status()
        if not response.json().get("boolean"):
            raise RuntimeError(f"WebHDFS không tạo được thư mục {path}")

    def rename(self, source: str, destination: str) -> None:
        response = requests.put(
            self._url(source, "RENAME", destination=destination),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        if not response.json().get("boolean"):
            raise RuntimeError(f"WebHDFS không rename được {source} -> {destination}")


def validate_staged_report(client: WebHdfsClient, staging_uri: str) -> dict[str, object]:
    root = hdfs_path(staging_uri)
    required = (
        f"{root}/requests_per_minute/_SUCCESS",
        f"{root}/status_distribution/_SUCCESS",
    )
    missing = [path for path in required if not client.exists(path)]
    if missing:
        raise RuntimeError(
            "HDFS staging report chưa hoàn chỉnh: " + ", ".join(missing)
        )
    return {"passed": True, "artifacts": len(required), "staging_uri": staging_uri}


def publish_staged_report(
    client: WebHdfsClient,
    paths: ReportArtifactPaths,
) -> dict[str, object]:
    source = hdfs_path(paths.staging_uri)
    destination = hdfs_path(paths.published_uri)
    source_exists = client.exists(source)
    destination_exists = client.exists(destination)
    if destination_exists and not source_exists:
        validate_staged_report(client, paths.published_uri)
        return {
            "mode": "already-published",
            "published_uri": paths.published_uri,
        }
    if destination_exists:
        raise RuntimeError(
            "Published HDFS generation đã tồn tại; dùng DAG run_id mới thay vì "
            "ghi đè artifact đã công bố"
        )
    if not source_exists:
        raise RuntimeError("Không tìm thấy HDFS staging report để publish")
    validate_staged_report(client, paths.staging_uri)
    client.mkdirs(destination.rsplit("/", 1)[0])
    client.rename(source, destination)
    if not client.exists(destination):
        raise RuntimeError("Atomic rename trả thành công nhưng destination chưa tồn tại")
    return {"mode": "atomic-rename", "published_uri": paths.published_uri}
