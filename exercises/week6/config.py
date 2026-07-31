from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


ALLOWED_SCENARIOS = {
    "success",
    "rate_limit",
    "server_error",
    "transient_500",
    "timeout",
    "malformed_json",
    "invalid_record",
    "duplicate",
    "empty",
}
MAX_BACKFILL_WINDOW = timedelta(days=31)
BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$")


class ConfigurationError(ValueError):
    """Cấu hình DAG không hợp lệ."""


@dataclass(frozen=True)
class RunConfiguration:
    run_id: str
    batch_id: str
    window_start: str
    window_end: str
    scenario: str
    invalid_rate_threshold: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_utc(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"{field_name} phải là thời gian ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ConfigurationError(f"{field_name} phải có múi giờ")
    return parsed.astimezone(timezone.utc)


def resolve_run_configuration(
    *,
    run_id: str,
    conf: dict[str, Any] | None,
    data_interval_start: datetime,
    data_interval_end: datetime,
    default_invalid_rate_threshold: float = 0.0,
) -> RunConfiguration:
    values = conf or {}
    window_start = parse_utc(values.get("window_start", data_interval_start), "window_start")
    window_end = parse_utc(values.get("window_end", data_interval_end), "window_end")
    if window_end <= window_start:
        raise ConfigurationError("window_end phải sau window_start")
    if window_end - window_start > MAX_BACKFILL_WINDOW:
        raise ConfigurationError("Mỗi backfill window không được vượt quá 31 ngày")

    batch_id = str(
        values.get(
            "batch_id",
            f"week6-{window_start:%Y%m%dT%H%M}-{window_end:%Y%m%dT%H%M}",
        )
    )
    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise ConfigurationError(
            "batch_id chỉ được chứa chữ, số, dấu chấm, gạch dưới, gạch ngang và tối đa 120 ký tự"
        )

    scenario = str(values.get("scenario", "success"))
    if scenario not in ALLOWED_SCENARIOS:
        raise ConfigurationError(f"scenario không được hỗ trợ: {scenario}")

    try:
        invalid_rate_threshold = float(
            values.get("invalid_rate_threshold", default_invalid_rate_threshold)
        )
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("invalid_rate_threshold phải là số") from exc
    if not 0 <= invalid_rate_threshold <= 1:
        raise ConfigurationError("invalid_rate_threshold phải nằm trong [0, 1]")

    return RunConfiguration(
        run_id=run_id,
        batch_id=batch_id,
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        scenario=scenario,
        invalid_rate_threshold=invalid_rate_threshold,
    )
