"""Tests for the core cron tick and interval math in `dlt.common.interval`."""

from datetime import datetime  # noqa: I251
from typing import Tuple
from zoneinfo import ZoneInfo

import pytest

from dlt.common.interval import full_days_interval, lag_cron, lag_interval
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import TTimeInterval

from tests.utils import make_interval

_BERLIN = ZoneInfo("Europe/Berlin")


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


def test_lag_cron_keeps_wall_clock_across_dst() -> None:
    """Ticks are computed on the wall clock of the input timezone, so a daily tick stays at
    midnight across a DST transition even though the offset changes."""
    dt = datetime(2024, 3, 31, 14, tzinfo=_BERLIN)
    lagged = lag_cron("0 0 * * *", dt, 1)
    assert lagged == datetime(2024, 3, 30, tzinfo=_BERLIN)
    assert lagged.tzinfo == _BERLIN
    assert lagged.utcoffset() != dt.utcoffset()


_LAG_IV = make_interval("2024-01-13T07:00:00Z", "2024-01-15T14:00:00Z")


@pytest.mark.parametrize(
    "trigger,count,lag_end,expected",
    [
        ("0 0 * * *", 3, False, ("2024-01-10T00:00:00Z", "2024-01-15T14:00:00Z")),
        ("schedule:0 0 * * *", 0, False, ("2024-01-13T00:00:00Z", "2024-01-15T14:00:00Z")),
        # lag_end drops the incomplete trailing day
        ("0 0 * * *", 0, True, ("2024-01-13T07:00:00Z", "2024-01-15T00:00:00Z")),
        ("0 0 * * *", -1, True, ("2024-01-13T07:00:00Z", "2024-01-16T00:00:00Z")),
        # every: shifts by whole periods, so count 0 is a no-op
        ("every:1h", 2, False, ("2024-01-13T05:00:00Z", "2024-01-15T14:00:00Z")),
        ("every:1h", 0, False, ("2024-01-13T07:00:00Z", "2024-01-15T14:00:00Z")),
    ],
    ids=[
        "bare-cron-start",
        "schedule-floor-start",
        "lag-end-floor",
        "lag-end-forward",
        "every-period",
        "every-noop",
    ],
)
def test_lag_interval(trigger: str, count: int, lag_end: bool, expected: Tuple[str, str]) -> None:
    assert lag_interval(_LAG_IV, trigger, count, lag_end) == make_interval(*expected)


@pytest.mark.parametrize(
    "trigger,count,lag_end,match",
    [
        ("0 0 * * *", 3, True, "empty or negative"),
        ("http:", 1, False, "no time period"),
    ],
    ids=["end-below-start", "no-period"],
)
def test_lag_interval_rejects(trigger: str, count: int, lag_end: bool, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        lag_interval(_LAG_IV, trigger, count, lag_end)


@pytest.mark.parametrize(
    "interval,expected",
    [
        pytest.param(
            make_interval("2024-01-13T07:00:00Z", "2024-01-15T14:00:00Z"),
            make_interval("2024-01-13T00:00:00Z", "2024-01-16T00:00:00Z"),
            id="mid-day-widened",
        ),
        pytest.param(
            make_interval("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z"),
            make_interval("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z"),
            id="aligned-unchanged",
        ),
        pytest.param(
            make_interval("2024-01-15T00:00:00Z", "2024-01-16T00:00:00.000001Z"),
            make_interval("2024-01-15T00:00:00Z", "2024-01-17T00:00:00Z"),
            id="sub-second-rounds-up",
        ),
        # bounds floor on the wall clock of their own timezone
        pytest.param(
            TTimeInterval(
                datetime(2024, 1, 15, 7, tzinfo=_BERLIN), datetime(2024, 1, 15, 14, tzinfo=_BERLIN)
            ),
            TTimeInterval(
                datetime(2024, 1, 15, tzinfo=_BERLIN), datetime(2024, 1, 16, tzinfo=_BERLIN)
            ),
            id="berlin-wall-clock",
        ),
        # spring-forward day: both midnights stay on the wall clock, so the day is 23 hours
        pytest.param(
            TTimeInterval(
                datetime(2024, 3, 31, 5, tzinfo=_BERLIN), datetime(2024, 3, 31, 9, tzinfo=_BERLIN)
            ),
            TTimeInterval(
                datetime(2024, 3, 31, tzinfo=_BERLIN), datetime(2024, 4, 1, tzinfo=_BERLIN)
            ),
            id="berlin-dst-spring-forward",
        ),
    ],
)
def test_full_days_interval(interval: TTimeInterval, expected: TTimeInterval) -> None:
    widened = full_days_interval(interval)
    assert widened == expected
    # a whole-days interval is already its own widening
    assert full_days_interval(widened) == widened
