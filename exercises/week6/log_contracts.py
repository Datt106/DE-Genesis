"""Hợp đồng thuần Python cho pipeline log Tuần 6.

Các hàm trong mô-đun này không phụ thuộc Spark, Kafka hay PostgreSQL để có
thể kiểm thử quy tắc cửa sổ, phân vùng HDFS và chất lượng dữ liệu ngay trên
máy phát triển.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any


ROTATION_MINUTES = 5
MAX_MICRO_BATCH_SECONDS = 60
DEFAULT_MICRO_BATCH_SECONDS = 30
DEFAULT_MAX_EVENT_DELAY_SECONDS = 120
DEFAULT_REPORT_SETTLEMENT_SECONDS = 180
MAX_REPORT_BACKFILL = timedelta(days=7)
ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
GENERATION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")


class LogContractError(ValueError):
    """Dữ liệu hoặc cấu hình log vi phạm hợp đồng đã công bố."""


@dataclass(frozen=True)
class LogReportConfiguration:
    run_id: str
    window_start: str
    window_end: str
    mode: str
    settlement_seconds: int
    data_available_through: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_utc(value: Any, field_name: str = "timestamp") -> datetime:
    """Chuyển giá trị ISO-8601 có múi giờ về UTC."""

    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise LogContractError(f"{field_name} phải là thời gian ISO-8601") from exc
    if parsed.tzinfo is None:
        raise LogContractError(f"{field_name} phải có múi giờ")
    return parsed.astimezone(timezone.utc)


def floor_to_rotation(value: Any, minutes: int = ROTATION_MINUTES) -> datetime:
    """Lấy đầu cửa sổ cố định dùng để xoay phân vùng raw trên HDFS."""

    if minutes <= 0 or 60 % minutes:
        raise LogContractError("Số phút xoay phải là ước dương của 60")
    timestamp = parse_utc(value, "event_time")
    return timestamp.replace(
        minute=(timestamp.minute // minutes) * minutes,
        second=0,
        microsecond=0,
    )


def raw_partition_path(base_path: str, value: Any) -> str:
    """Sinh đường dẫn phân vùng raw xác định theo cửa sổ 5 phút."""

    rotation = floor_to_rotation(value)
    return (
        f"{base_path.rstrip('/')}"
        f"/ingest_date={rotation:%Y-%m-%d}"
        f"/ingest_hour={rotation:%H}"
        f"/rotation_5m={rotation:%Y%m%dT%H%MZ}"
    )


def validate_micro_batch_seconds(value: Any) -> int:
    """Bảo vệ SLA: một micro-batch phải chạy ít nhất mỗi 60 giây."""

    try:
        seconds = int(value)
    except (TypeError, ValueError) as exc:
        raise LogContractError("micro_batch_seconds phải là số nguyên") from exc
    if not 1 <= seconds <= MAX_MICRO_BATCH_SECONDS:
        raise LogContractError("micro_batch_seconds phải nằm trong [1, 60]")
    return seconds


def validate_stream_generation_id(value: Any) -> str:
    """Generation phải đổi khi checkpoint được tạo lại từ đầu."""

    generation_id = str(value).strip()
    if not GENERATION_PATTERN.fullmatch(generation_id):
        raise LogContractError(
            "stream_generation_id chỉ nhận chữ, số, dấu chấm, gạch dưới, "
            "gạch ngang và tối đa 80 ký tự"
        )
    return generation_id


def stream_batch_sequence_action(
    stream_batch_id: int,
    last_successful_batch_id: int,
    existing_status: str | None = None,
) -> str:
    """Quyết định process/replay và chặn epoch quay lùi hoặc nhảy cóc."""

    if stream_batch_id < last_successful_batch_id:
        raise LogContractError(
            f"Checkpoint quay lùi từ batch {last_successful_batch_id} "
            f"về {stream_batch_id}"
        )
    if stream_batch_id == last_successful_batch_id:
        if existing_status == "success":
            return "replay"
        raise LogContractError("High-water mark generation không khớp telemetry")
    if (
        last_successful_batch_id >= 0
        and stream_batch_id > last_successful_batch_id + 1
    ):
        raise LogContractError("Batch ID nhảy cóc trong cùng checkpoint generation")
    return "process"


def select_stream_lineage_id(
    *,
    stream_generation_id: str,
    stream_batch_id: int,
    active_last_successful_batch_id: int | None,
    active_lineage_id: str | None,
) -> str:
    """Giữ lineage khi epoch nối tiếp; reset epoch tạo lineage thay thế."""

    if (
        active_last_successful_batch_id is not None
        and active_lineage_id
        and stream_batch_id == active_last_successful_batch_id + 1
    ):
        return active_lineage_id
    return stream_generation_id


def validate_log_event(payload: Any) -> list[str]:
    """Trả về toàn bộ lỗi blocking của một service-log event."""

    if not isinstance(payload, dict):
        return ["payload phải là JSON object"]

    errors: list[str] = []
    for field in ("event_id", "event_time", "service", "method", "path"):
        if payload.get(field) in (None, ""):
            errors.append(f"{field} không được để trống")

    method = str(payload.get("method", "")).upper()
    if method and method not in ALLOWED_METHODS:
        errors.append("method không hợp lệ")

    try:
        status_code = int(payload.get("status_code"))
        if not 100 <= status_code <= 599:
            errors.append("status_code phải nằm trong [100, 599]")
    except (TypeError, ValueError):
        errors.append("status_code phải là số nguyên")

    try:
        latency_ms = int(payload.get("latency_ms"))
        if latency_ms < 0:
            errors.append("latency_ms không được âm")
    except (TypeError, ValueError):
        errors.append("latency_ms phải là số nguyên")

    if payload.get("event_time") not in (None, ""):
        try:
            parse_utc(payload["event_time"], "event_time")
        except LogContractError as exc:
            errors.append(str(exc))
    return errors


def resolve_log_report_configuration(
    *,
    run_id: str,
    conf: dict[str, Any] | None,
    data_interval_start: datetime,
    data_interval_end: datetime,
    settlement_seconds: int = DEFAULT_REPORT_SETTLEMENT_SECONDS,
    max_event_delay_seconds: int = DEFAULT_MAX_EVENT_DELAY_SECONDS,
    micro_batch_seconds: int = DEFAULT_MICRO_BATCH_SECONDS,
    reference_time: datetime | None = None,
) -> LogReportConfiguration:
    """Chuẩn hóa cửa sổ report/backfill của DAG log."""

    values = conf or {}
    micro_batch_seconds = validate_micro_batch_seconds(micro_batch_seconds)
    if max_event_delay_seconds <= 0:
        raise LogContractError("max_event_delay_seconds phải lớn hơn 0")
    if settlement_seconds < max_event_delay_seconds + micro_batch_seconds:
        raise LogContractError(
            "settlement_seconds phải ít nhất bằng max_event_delay_seconds + "
            "micro_batch_seconds"
        )
    explicit_window = "window_start" in values or "window_end" in values
    interval_start = parse_utc(data_interval_start, "data_interval_start")
    interval_end = parse_utc(data_interval_end, "data_interval_end")
    if explicit_window:
        start = parse_utc(values.get("window_start", interval_start), "window_start")
        end = parse_utc(values.get("window_end", interval_end), "window_end")
        observed_at = parse_utc(
            reference_time or datetime.now(timezone.utc), "reference_time"
        )
        if end + timedelta(seconds=settlement_seconds) > observed_at:
            raise LogContractError(
                "window_end chưa qua settlement delay; event hợp lệ đến trễ "
                "vẫn có thể chưa được ghi raw"
            )
        data_available_through = floor_to_rotation(
            observed_at - timedelta(seconds=settlement_seconds)
        )
    else:
        interval_duration = interval_end - interval_start
        if interval_duration <= timedelta(0):
            raise LogContractError("Data interval của lịch chạy không hợp lệ")
        data_available_through = floor_to_rotation(
            interval_end - timedelta(seconds=settlement_seconds)
        )
        end = data_available_through
        start = end - interval_duration
    if end <= start:
        raise LogContractError("window_end phải sau window_start")
    if end - start > MAX_REPORT_BACKFILL:
        raise LogContractError("Mỗi backfill log không được vượt quá 7 ngày")
    if any((start.second, start.microsecond, end.second, end.microsecond)):
        raise LogContractError("Cửa sổ báo cáo phải căn theo phút")
    return LogReportConfiguration(
        run_id=run_id,
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        mode="backfill" if explicit_window else "scheduled",
        settlement_seconds=settlement_seconds,
        data_available_through=data_available_through.isoformat(),
    )


def align_log_report_window_to_watermark(
    config: LogReportConfiguration,
    current_watermark: datetime | str | None,
) -> LogReportConfiguration:
    """Lấp các cửa sổ report bị scheduler bỏ qua bằng watermark thành công."""

    if config.mode != "scheduled" or current_watermark is None:
        return config
    watermark = parse_utc(current_watermark, "current_watermark")
    window_end = parse_utc(config.window_end, "window_end")
    if watermark >= window_end:
        raise LogContractError("Watermark report đã bằng hoặc vượt window_end")
    if window_end - watermark > MAX_REPORT_BACKFILL:
        raise LogContractError("Gap report vượt 7 ngày; phải backfill thành nhiều cửa sổ")
    return replace(config, window_start=watermark.isoformat())


def calculate_ingestion_lag_seconds(event_time: Any, landed_at: Any) -> float:
    """Đo độ trễ event-to-HDFS, chặn giá trị âm do lệch đồng hồ nhỏ."""

    event = parse_utc(event_time, "event_time")
    landed = parse_utc(landed_at, "landed_at")
    return max(0.0, (landed - event).total_seconds())


def normalize_spark_timestamp_utc(value: datetime | None) -> datetime | None:
    """Chuẩn hóa datetime do Spark trả về; Spark bỏ tzinfo dù session dùng UTC."""

    if value is None:
        return None
    if not isinstance(value, datetime):
        raise LogContractError("Spark timestamp phải là datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def calculate_batch_ingestion_lag_seconds(
    event_times: list[Any], raw_committed_at: Any
) -> float | None:
    """Trả độ trễ tệ nhất, không dùng event mới nhất để che event bị trễ."""

    if not event_times:
        return None
    committed = parse_utc(raw_committed_at, "raw_committed_at")
    return max(
        calculate_ingestion_lag_seconds(event_time, committed)
        for event_time in event_times
    )
