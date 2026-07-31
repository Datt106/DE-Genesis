from datetime import datetime, timezone

import pytest

from exercises.week6.config import ConfigurationError, resolve_run_configuration


START = datetime(2026, 7, 20, tzinfo=timezone.utc)
END = datetime(2026, 7, 21, tzinfo=timezone.utc)


def test_default_configuration_is_deterministic() -> None:
    config = resolve_run_configuration(
        run_id="scheduled__2026-07-20",
        conf={},
        data_interval_start=START,
        data_interval_end=END,
    )
    assert config.batch_id == "week6-20260720T0000-20260721T0000"
    assert config.scenario == "success"
    assert config.window_start == "2026-07-20T00:00:00+00:00"
    assert config.invalid_rate_threshold == 0


def test_manual_backfill_configuration() -> None:
    config = resolve_run_configuration(
        run_id="manual__backfill",
        conf={
            "batch_id": "backfill-20260720",
            "window_start": "2026-07-20T00:00:00Z",
            "window_end": "2026-07-21T00:00:00Z",
            "invalid_rate_threshold": 0.01,
        },
        data_interval_start=START,
        data_interval_end=END,
    )
    assert config.batch_id == "backfill-20260720"
    assert config.invalid_rate_threshold == 0.01


@pytest.mark.parametrize(
    "conf",
    [
        {"window_start": "2026-07-21T00:00:00Z", "window_end": "2026-07-20T00:00:00Z"},
        {"window_start": "2026-01-01T00:00:00Z", "window_end": "2026-03-01T00:00:00Z"},
        {"batch_id": "../../unsafe"},
        {"scenario": "unknown"},
        {"invalid_rate_threshold": 1.1},
    ],
)
def test_invalid_configuration_is_rejected(conf) -> None:
    with pytest.raises(ConfigurationError):
        resolve_run_configuration(
            run_id="manual__invalid",
            conf=conf,
            data_interval_start=START,
            data_interval_end=END,
        )
