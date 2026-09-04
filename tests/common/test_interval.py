"""Tests for the core cron tick and interval math in `dlt.common.interval`."""

from datetime import datetime  # noqa: I251
from zoneinfo import ZoneInfo

import pytest

from dlt.common.interval import full_days_interval, lag_cron, lag_interval
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TTimeInterval

from tests.utils import make_interval


@pytest.mark.parametrize(
    "cron,dt,count,expected",
    [
        # daily cron, off-tick value
        ("0 0 * * *", "2024-01-15T14:00:00Z", 0, "2024-01-15T00:00:00Z"),
        ("0 0 * * *", "2024-01-15T14:00:00Z", 1, "2024-01-14T00:00:00Z"),
        ("0 0 * * *", "2024-01-15T14:00:00Z", 3, "2024-01-12T00:00:00Z"),
        ("0 0 * * *", "2024-01-15T14:00:00Z", -1, "2024-01-16T00:00:00Z"),
        ("0 0 * * *", "2024-01-15T14:00:00Z", -2, "2024-01-17T00:00:00Z"),
        # daily cron, exactly on a tick
        ("0 0 * * *", "2024-01-15T00:00:00Z", 0, "2024-01-15T00:00:00Z"),
        ("0 0 * * *", "2024-01-15T00:00:00Z", 1, "2024-01-14T00:00:00Z"),
        ("0 0 * * *", "2024-01-15T00:00:00Z", -1, "2024-01-16T00:00:00Z"),
        # hourly cron
        ("0 * * * *", "2024-01-15T14:30:00Z", 2, "2024-01-15T12:00:00Z"),
        ("0 * * * *", "2024-01-15T14:30:00Z", -1, "2024-01-15T15:00:00Z"),
        # sub-hour grids, where a seconds-wide epsilon would overshoot the floor
        ("*/3 * * * *", "2024-01-01T11:40:00Z", 0, "2024-01-01T11:39:00Z"),
        ("*/3 * * * *", "2024-01-01T11:42:00Z", 0, "2024-01-01T11:42:00Z"),
        ("* * * * *", "2024-01-01T11:40:00Z", 0, "2024-01-01T11:40:00Z"),
        ("* * * * *", "2024-01-01T11:59:59.999999Z", 0, "2024-01-01T11:59:00Z"),
    ],
    ids=[
        "daily-floor",
        "daily-back-1",
        "daily-back-3",
        "daily-forward-1",
        "daily-forward-2",
        "on-tick-floor",
        "on-tick-back-1",
        "on-tick-forward-1",
        "hourly-back-2",
        "hourly-forward-1",
        "3min-misaligned",
        "3min-aligned",
        "1min-aligned",
        "1min-sub-second",
    ],
)
def test_lag_cron(cron: str, dt: str, count: int, expected: str) -> None:
    assert lag_cron(cron, ensure_datetime_in_tz(dt), count) == ensure_datetime_in_tz(expected)


def test_lag_cron_preserves_timezone() -> None:
    """Ticks are computed on the wall clock of the input timezone."""
    berlin = ZoneInfo("Europe/Berlin")
    dt = datetime(2024, 1, 15, 14, 0, tzinfo=berlin)
    lagged = lag_cron("0 0 * * *", dt, 1)
    assert lagged == datetime(2024, 1, 14, 0, 0, tzinfo=berlin)
    assert lagged.tzinfo is berlin


def test_lag_interval() -> None:
    iv = make_interval("2024-01-13T07:00:00Z", "2024-01-15T14:00:00Z")

    # bare cron lags the start, end untouched
    assert lag_interval(iv, "0 0 * * *", 3) == make_interval(
        "2024-01-10T00:00:00Z", "2024-01-15T14:00:00Z"
    )
    # schedule: trigger form, count 0 floors the start
    assert lag_interval(iv, "schedule:0 0 * * *", 0) == make_interval(
        "2024-01-13T00:00:00Z", "2024-01-15T14:00:00Z"
    )
    # lag_end drops the incomplete trailing day
    assert lag_interval(iv, "0 0 * * *", 0, lag_end=True) == make_interval(
        "2024-01-13T07:00:00Z", "2024-01-15T00:00:00Z"
    )
    # negative count moves the bound into the future
    assert lag_interval(iv, "0 0 * * *", -1, lag_end=True) == make_interval(
        "2024-01-13T07:00:00Z", "2024-01-16T00:00:00Z"
    )
    # every: trigger shifts by period, count 0 is a no-op
    assert lag_interval(iv, "every:1h", 2) == make_interval(
        "2024-01-13T05:00:00Z", "2024-01-15T14:00:00Z"
    )
    assert lag_interval(iv, "every:1h", 0) == iv

    # lagging the end below the start raises
    with pytest.raises(ValueError, match="empty or negative"):
        lag_interval(iv, "0 0 * * *", 3, lag_end=True)
    # period-less triggers raise
    with pytest.raises(ValueError, match="no time period"):
        lag_interval(iv, "http:", 1)


def test_full_days_interval() -> None:
    # mid-day bounds widened to cover both days fully
    assert full_days_interval(
        make_interval("2024-01-13T07:00:00Z", "2024-01-15T14:00:00Z")
    ) == make_interval("2024-01-13T00:00:00Z", "2024-01-16T00:00:00Z")
    # an interval already covering whole days is returned unchanged, so widening is idempotent
    aligned = make_interval("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    assert full_days_interval(aligned) == aligned
    assert full_days_interval(full_days_interval(aligned)) == aligned
    # a sub-second past midnight still rounds up to the next one
    assert full_days_interval(
        make_interval("2024-01-15T00:00:00Z", "2024-01-16T00:00:00.000001Z")
    ) == make_interval("2024-01-15T00:00:00Z", "2024-01-17T00:00:00Z")
    # bounds floor on the wall clock of their own timezone
    berlin = ZoneInfo("Europe/Berlin")
    widened = full_days_interval(
        TTimeInterval(
            datetime(2024, 1, 15, 7, 0, tzinfo=berlin), datetime(2024, 1, 15, 14, 0, tzinfo=berlin)
        )
    )
    assert widened == TTimeInterval(
        datetime(2024, 1, 15, 0, 0, tzinfo=berlin), datetime(2024, 1, 16, 0, 0, tzinfo=berlin)
    )
