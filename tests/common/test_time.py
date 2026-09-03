import time
from datetime import datetime, date, timezone, timedelta, time as dt_time  # noqa: I251
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
from pendulum.tz import UTC, fixed_timezone

from dlt.common import pendulum
from dlt.common.pendulum import ensure_pendulum_dt, to_pendulum_tz
from dlt.common.storages.load_package import create_load_id
from dlt.common.time import (
    MonotonicPreciseTime,
    LockedMonotonicPreciseTime,
    date_to_epoch_days,
    increasing_precise_time,
    precise_time,
    parse_iso_like_datetime,
    timestamp_before,
    timestamp_within,
    ensure_datetime,
    ensure_datetime_in_tz,
    ensure_pendulum_datetime,
    ensure_pendulum_date,
    datetime_to_timestamp,
    datetime_to_timestamp_ms,
    datetime_to_timestamp_us,
    detect_datetime_format,
    ensure_pendulum_time,
    normalize_timezone,
    set_context_timezone,
    datetime_obj_to_str,
    to_iana_name,
)
from dlt.common.typing import TAnyDateTime

from tests.utils import LOCAL_TIMEZONES, local_timezone


def test_timestamp_within() -> None:
    assert timestamp_within(1643470504.782716, 1643470504.782716, 1643470504.782716) is False
    # true for all timestamps
    assert timestamp_within(1643470504.782716, None, None) is True
    # upper bound inclusive
    assert timestamp_within(1643470504.782716, None, 1643470504.782716) is True
    # lower bound exclusive
    assert timestamp_within(1643470504.782716, 1643470504.782716, None) is False
    assert timestamp_within(1643470504.782716, 1643470504.782715, None) is True
    assert timestamp_within(1643470504.782716, 1643470504.782715, 1643470504.782716) is True
    # typical case
    assert timestamp_within(1643470504.782716, 1543470504.782716, 1643570504.782716) is True


def test_before() -> None:
    # True for all timestamps
    assert timestamp_before(1643470504.782716, None) is True
    # inclusive
    assert timestamp_before(1643470504.782716, 1643470504.782716) is True
    # typical cases
    assert timestamp_before(1643470504.782716, 1643470504.782717) is True
    assert timestamp_before(1643470504.782716, 1643470504.782715) is False


# Test parameters for datetime conversion functions
# Format: (input_value, expected_utc_datetime, expected_non_utc_datetime, expected_date)
datetime_test_params = [
    # python datetime without tz - naive datetime treated as UTC
    (
        datetime(2021, 1, 1, 0, 0, 0),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0),  # remains naive
        pendulum.date(2021, 1, 1),
    ),
    # python datetime with negative timezone offset
    (
        datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
        pendulum.DateTime(2021, 1, 1, 8, 0, 0).in_tz("UTC"),  # converted to UTC
        pendulum.DateTime(
            2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-8))
        ),  # preserves original tz
        pendulum.date(2021, 1, 1),  # date in UTC
    ),
    # python datetime with positive timezone offset
    (
        datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8))),
        pendulum.DateTime(2020, 12, 31, 16, 0, 0).in_tz("UTC"),  # converted to UTC
        pendulum.DateTime(
            2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8))
        ),  # preserves original tz
        pendulum.date(2020, 12, 31),  # date in UTC
    ),
    # python date object
    (
        date(2021, 1, 1),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0),  # naive datetime
        pendulum.date(2021, 1, 1),
    ),
    # pendulum datetime with UTC tz
    (
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),  # preserves UTC
        pendulum.date(2021, 1, 1),
    ),
    # pendulum datetime without tz (naive)
    (
        pendulum.DateTime(2021, 1, 1, 0, 0, 0),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),  # assumes UTC
        pendulum.DateTime(2021, 1, 1, 0, 0, 0),  # remains naive
        pendulum.date(2021, 1, 1),
    ),
    # pendulum datetime with non-UTC timezone
    (
        pendulum.DateTime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=5))),
        pendulum.DateTime(2020, 12, 31, 19, 0, 0).in_tz("UTC"),  # converted to UTC
        pendulum.DateTime(
            2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=5))
        ),  # preserves original tz
        pendulum.date(2020, 12, 31),  # date in UTC
    ),
    # iso datetime in UTC
    (
        "2021-01-01T00:00:00+00:00",
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),  # preserves UTC from string
        pendulum.date(2021, 1, 1),
    ),
    # iso datetime with positive timezone offset
    (
        "2021-01-01T00:00:00+05:00",
        pendulum.DateTime(2020, 12, 31, 19, 0, 0).in_tz("UTC"),  # converted to UTC
        pendulum.datetime(2021, 1, 1, 0, 0, 0, tz=5),  # preserves original tz from string
        pendulum.date(2020, 12, 31),  # date in UTC
    ),
    # iso datetime without tz - treated as naive/UTC
    (
        "2021-01-01T05:02:32",
        pendulum.DateTime(2021, 1, 1, 5, 2, 32).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 5, 2, 32),  # remains naive
        pendulum.date(2021, 1, 1),
    ),
    # iso date string
    (
        "2021-01-01",
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0),  # naive datetime
        pendulum.date(2021, 1, 1),
    ),
    # unix timestamp as int
    (
        1609459200,  # 2021-01-01T00:00:00 UTC
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),  # timestamps always have UTC tz
        pendulum.date(2021, 1, 1),
    ),
    # unix timestamp as float with microseconds
    (
        1609459200.123456,
        pendulum.DateTime(2021, 1, 1, 0, 0, 0, 123456).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0, 123456).in_tz(
            "UTC"
        ),  # timestamps always have UTC tz
        pendulum.date(2021, 1, 1),
    ),
    # unix timestamp as string
    (
        "1609459200",
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),  # timestamps always have UTC tz
        pendulum.date(2021, 1, 1),
    ),
    # unix timestamp as float string
    (
        "1609459200.123456",
        pendulum.DateTime(2021, 1, 1, 0, 0, 0, 123456).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0, 123456).in_tz(
            "UTC"
        ),  # timestamps always have UTC tz
        pendulum.date(2021, 1, 1),
    ),
]


def test_parse_iso_like_datetime() -> None:
    # naive datetime is still naive
    assert parse_iso_like_datetime("2021-01-01T05:02:32") == pendulum.DateTime(2021, 1, 1, 5, 2, 32)
    assert parse_iso_like_datetime("2021-01-01T05:02:32+08:00") == pendulum.DateTime(
        2021, 1, 1, 5, 2, 32, tzinfo=fixed_timezone(8 * 60 * 60)
    )
    # test that _parse_common form pendulum parsing is not failing with KeyError
    assert parse_iso_like_datetime("2021:01:01 05:02:32") == pendulum.DateTime(2021, 1, 1, 5, 2, 32)
    # assert parse_iso_like_datetime("2021:01:01 05:02:32+08:00") == pendulum.DateTime(2021, 1, 1, 5, 2, 32)


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "date_value, expected_utc, expected_non_utc, expected_date", datetime_test_params
)
def test_ensure_pendulum_datetime_defaults_to_utc(
    local_tz: str,
    date_value: TAnyDateTime,
    expected_utc: pendulum.DateTime,
    expected_non_utc: pendulum.DateTime,
    expected_date: pendulum.Date,
) -> None:
    with local_timezone(local_tz):
        dt = ensure_pendulum_datetime(date_value)
        assert dt == expected_utc
        # always UTC
        assert dt.tz == UTC
        # always pendulum
        assert isinstance(dt, pendulum.DateTime)
        # NOTE: pendulum destroys timezone information, here we make sure we don't do that
        # works with timedelta
        dt_add = dt + timedelta(days=1)
        assert dt_add.tz == UTC
        # works with add()
        assert dt.add(days=1).tz == UTC


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "date_value, expected_utc, expected_non_utc, expected_date", datetime_test_params
)
def test_ensure_datetime_preserves_tz(
    local_tz: str,
    date_value: TAnyDateTime,
    expected_utc: pendulum.DateTime,
    expected_non_utc: pendulum.DateTime,
    expected_date: pendulum.Date,
) -> None:
    with local_timezone(local_tz):
        dt = ensure_datetime(date_value)
        assert dt == expected_non_utc

        def _test_tz(dt_: datetime) -> None:
            # timezone awareness preserved
            if dt_.tzinfo or expected_non_utc.tzinfo:
                assert dt_.tzinfo.utcoffset(dt_) == expected_non_utc.tzinfo.utcoffset(
                    expected_non_utc
                )
            else:
                assert dt_.tzinfo is expected_non_utc.tzinfo is None

        _test_tz(dt)
        # a stdlib datetime, and arithmetic keeps the original offset
        assert type(dt) is datetime
        _test_tz(dt + timedelta(days=1))


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "date_value, expected_utc, expected_non_utc, expected_date", datetime_test_params
)
def test_ensure_pendulum_date(
    local_tz: str,
    date_value: TAnyDateTime,
    expected_utc: pendulum.DateTime,
    expected_non_utc: pendulum.DateTime,
    expected_date: pendulum.Date,
) -> None:
    with local_timezone(local_tz):
        dt = ensure_pendulum_date(date_value)
        assert dt == expected_date
        # always pendulum date
        assert isinstance(dt, pendulum.Date)


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
def test_ensure_pendulum_date_utc(local_tz: str) -> None:
    """Additional specific test cases for ensure_pendulum_date"""

    with local_timezone(local_tz):
        # when converting from datetimes make sure to shift to UTC before doing date
        assert ensure_pendulum_date("2021-01-01T00:00:00+05:00") == pendulum.date(2020, 12, 31)
        assert ensure_pendulum_date(
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))
        ) == pendulum.date(2020, 12, 31)

        # pendulum datetime with timezone
        assert ensure_pendulum_date(
            pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz(timezone(timedelta(hours=5)))  # type: ignore[arg-type]
        ) == pendulum.date(2020, 12, 31)

        # naive datetime should be treated as UTC
        assert ensure_pendulum_date(datetime(2021, 1, 1, 0, 0, 0)) == pendulum.date(2021, 1, 1)

        # pendulum date should pass through unchanged
        assert ensure_pendulum_date(pendulum.date(2021, 1, 1)) == pendulum.date(2021, 1, 1)

        # python date should pass through
        assert ensure_pendulum_date(date(2021, 1, 1)) == pendulum.date(2021, 1, 1)

        # iso date string
        assert ensure_pendulum_date("2021-01-01") == pendulum.date(2021, 1, 1)

        # unix timestamp as int
        assert ensure_pendulum_date(1609459200) == pendulum.date(2021, 1, 1)

        # unix timestamp as float
        assert ensure_pendulum_date(1609459200.5) == pendulum.date(2021, 1, 1)

        # unix timestamp as string
        assert ensure_pendulum_date("1609459200") == pendulum.date(2021, 1, 1)

        # pendulum datetime with timezone
        assert ensure_pendulum_date(
            pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz(timezone(timedelta(hours=5)))  # type: ignore[arg-type]
        ) == pendulum.date(2020, 12, 31)

        # naive datetime should be treated as UTC
        assert ensure_pendulum_date(datetime(2021, 1, 1, 0, 0, 0)) == pendulum.date(2021, 1, 1)

        # pendulum date should pass through unchanged
        assert ensure_pendulum_date(pendulum.date(2021, 1, 1)) == pendulum.date(2021, 1, 1)

        # python date should pass through
        assert ensure_pendulum_date(date(2021, 1, 1)) == pendulum.date(2021, 1, 1)

        # iso date string
        assert ensure_pendulum_date("2021-01-01") == pendulum.date(2021, 1, 1)

        # unix timestamp as int
        assert ensure_pendulum_date(1609459200) == pendulum.date(2021, 1, 1)

        # unix timestamp as float
        assert ensure_pendulum_date(1609459200.5) == pendulum.date(2021, 1, 1)

        # unix timestamp as string
        assert ensure_pendulum_date("1609459200") == pendulum.date(2021, 1, 1)


test_timestamps = [
    (pendulum.DateTime(2024, 4, 26, 5, 16, 22, 738029).in_tz("UTC"), 1714108582, 1714108582738),
    (pendulum.DateTime(2024, 4, 26, 6, 26, 22, 738029).in_tz("UTC"), 1714112782, 1714112782738),
    (pendulum.DateTime(2024, 4, 26, 7, 36, 22, 738029).in_tz("UTC"), 1714116982, 1714116982738),
    (pendulum.DateTime(2024, 4, 26, 8, 46, 22, 738029).in_tz("UTC"), 1714121182, 1714121182738),
    (pendulum.DateTime(2024, 4, 26, 9, 56, 22, 738029).in_tz("UTC"), 1714125382, 1714125382738),
    (pendulum.DateTime(2024, 4, 26, 11, 6, 22, 738029).in_tz("UTC"), 1714129582, 1714129582738),
    (pendulum.DateTime(2024, 4, 26, 12, 16, 22, 738029).in_tz("UTC"), 1714133782, 1714133782738),
    (pendulum.DateTime(2024, 4, 26, 13, 26, 22, 738029).in_tz("UTC"), 1714137982, 1714137982738),
    (pendulum.DateTime(2024, 4, 26, 14, 36, 22, 738029).in_tz("UTC"), 1714142182, 1714142182738),
    (pendulum.DateTime(2024, 4, 26, 15, 46, 22, 738029).in_tz("UTC"), 1714146382, 1714146382738),
]


@pytest.mark.parametrize("datetime_obj,timestamp,timestamp_ms", test_timestamps)
def test_datetime_to_timestamp_helpers(
    datetime_obj: pendulum.DateTime, timestamp: int, timestamp_ms: int
) -> None:
    assert datetime_to_timestamp(datetime_obj) == timestamp
    assert datetime_to_timestamp_ms(datetime_obj) == timestamp_ms
    assert datetime_to_timestamp_us(datetime_obj) == timestamp * 1_000_000 + 738029


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
def test_datetime_to_timestamp_ignores_os_timezone(local_tz: str) -> None:
    """A naive datetime is read as UTC, never in the machine timezone."""
    naive = datetime(2024, 1, 15, 23, 30, 0, 250000)
    # the same instant, spelled in three ways
    values = [
        naive,
        naive.replace(tzinfo=timezone.utc),
        naive.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Berlin")),
    ]
    with local_timezone(local_tz):
        for value in values:
            assert datetime_to_timestamp(value) == 1705361400
            assert datetime_to_timestamp_ms(value) == 1705361400250
            assert datetime_to_timestamp_us(value) == 1705361400250000


def test_datetime_to_timestamp_us_before_epoch() -> None:
    """Sub-second parts of a pre-epoch instant add to the microseconds, they do not cancel out."""
    before_epoch = datetime(1960, 1, 1, 0, 0, 0, 500000, tzinfo=timezone.utc)
    assert datetime_to_timestamp_us(before_epoch) == -315619199_500_000
    assert datetime_to_timestamp_ms(before_epoch) == -315619199_500


@pytest.mark.parametrize(
    "value, expected_format",
    [
        ("2024-10-20T15:30:00Z", "%Y-%m-%dT%H:%M:%SZ"),  # UTC 'Z'
        ("2024-10-20T15:30:00.123456Z", "%Y-%m-%dT%H:%M:%S.%fZ"),  # UTC 'Z' with fractional seconds
        ("2024-10-20T15:30:00+02:00", "%Y-%m-%dT%H:%M:%S%:z"),  # Positive timezone offset
        ("2024-10-20T15:30:00+0200", "%Y-%m-%dT%H:%M:%S%z"),  # Positive timezone offset (no colon)
        (
            "2024-10-20T15:30:00.123456+02:00",
            "%Y-%m-%dT%H:%M:%S.%f%:z",
        ),  # Positive timezone offset with fractional seconds
        (
            "2024-10-20T15:30:00.123456+0200",
            "%Y-%m-%dT%H:%M:%S.%f%z",
        ),  # Positive timezone offset with fractional seconds (no colon)
        ("2024-10-20T15:30:00-02:00", "%Y-%m-%dT%H:%M:%S%:z"),  # Negative timezone offset
        ("2024-10-20T15:30:00-0200", "%Y-%m-%dT%H:%M:%S%z"),  # Negative timezone offset (no colon)
        (
            "2024-10-20T15:30:00.123456-02:00",
            "%Y-%m-%dT%H:%M:%S.%f%:z",
        ),  # Negative timezone offset with fractional seconds
        (
            "2024-10-20T15:30:00.123456-0200",
            "%Y-%m-%dT%H:%M:%S.%f%z",
        ),  # Negative timezone offset with fractional seconds (no colon)
        ("2024-10-20T15:30:00", "%Y-%m-%dT%H:%M:%S"),  # No timezone
        ("2024-10-20T15:30", "%Y-%m-%dT%H:%M"),  # Minute precision
        ("2024-10-20T15", "%Y-%m-%dT%H"),  # Hour precision
        ("2024-10-20", "%Y-%m-%d"),  # Date only
        ("2024-10", "%Y-%m"),  # Year and month
        ("2024", "%Y"),  # Year only
        ("2024-W42", "%G-W%V"),  # Week-based date
        ("2024-W42-5", "%G-W%V-%u"),  # Week-based date with day
        ("2024-293", "%Y-%j"),  # Ordinal date
        ("20241020", "%Y%m%d"),  # Compact date format
        # ("202410", "%Y%m"),  # Compact year and month format NOTE: does not pass with pendulum < 3.0.0
    ],
)
def test_detect_datetime_format(value, expected_format) -> None:
    assert detect_datetime_format(value) == expected_format
    assert ensure_pendulum_datetime(value) is not None


@pytest.mark.parametrize(
    "value",
    [
        # ISO week dates around year boundaries where the ISO week-numbering year
        # differs from the calendar year. `%W`/`%Y` produce the wrong week here,
        # only `%V`/`%G` round-trip.
        "2026-W01",  # ISO week 1 of 2026 starts 2025-12-29
        "2026-W01-1",
        "2020-W53",  # 2020 has 53 ISO weeks
        "2021-W01",
        "2024-W42",
        "2024-W42-5",
    ],
)
def test_detect_datetime_format_week_roundtrip(value: str) -> None:
    # the detected format must reproduce the original string when used to render
    # the value parsed from it (this is how lag re-serializes string cursors)
    fmt = detect_datetime_format(value)
    parsed = ensure_datetime(value)
    assert datetime_obj_to_str(parsed, fmt) == value


@pytest.mark.parametrize(
    "datetime_str, datetime_format, expected_value",
    [
        ("2024-10-20T15:30:00+02:00", "%Y-%m-%dT%H:%M:%S%:z", "2024-10-20T15:30:00+02:00"),
        ("2024-10-20T15:30:00+0200", "%Y-%m-%dT%H:%M:%S%z", "2024-10-20T15:30:00+0200"),
        (
            "2024-10-20T15:30:00.123456-02:00",
            "%Y-%m-%dT%H:%M:%S.%f%:z",
            "2024-10-20T15:30:00.123456-02:00",
        ),
        (
            "2024-10-20T15:30:00.123456-0200",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "2024-10-20T15:30:00.123456-0200",
        ),
    ],
)
def test_datatime_obj_to_str(datetime_str, datetime_format, expected_value) -> None:
    datetime = ensure_datetime(datetime_str)
    assert datetime_obj_to_str(datetime, datetime_format) == expected_value


@pytest.mark.parametrize(
    "value",
    [
        "invalid-format",  # Invalid format
        "2024/10/32",  # Invalid format
        "2024-10-W",  # Invalid week format
        "2024-10-W42-8",  # Invalid day of the week
    ],
)
def test_detect_datetime_format_invalid(value) -> None:
    assert detect_datetime_format(value) is None
    with pytest.raises(ValueError):
        ensure_pendulum_datetime(value)


# Test parameters for normalize_timezone function
# Format: (input_datetime, timezone_param, expected_output, description)
normalize_timezone_test_params = [
    # Test with timezone=True (should always convert to UTC)
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0),  # naive datetime
        True,
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz("UTC"),  # should become UTC-aware
        "naive datetime with timezone=True",
    ),
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz("UTC"),  # already UTC-aware
        True,
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz("UTC"),  # should remain UTC-aware
        "UTC-aware datetime with timezone=True",
    ),
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz(
            timezone(timedelta(hours=5))  # type: ignore[arg-type]
        ),  # non-UTC timezone
        True,
        pendulum.DateTime(2021, 1, 1, 7, 0, 0).in_tz("UTC"),  # should convert to UTC
        "non-UTC timezone with timezone=True",
    ),
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz(
            timezone(timedelta(hours=-8))  # type: ignore[arg-type]
        ),  # negative offset
        True,
        pendulum.DateTime(2021, 1, 1, 20, 0, 0).in_tz("UTC"),  # should convert to UTC
        "negative timezone offset with timezone=True",
    ),
    # Test with timezone=False (should convert to naive UTC)
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0),  # naive datetime
        False,
        pendulum.DateTime(2021, 1, 1, 12, 0, 0),  # should remain naive
        "naive datetime with timezone=False",
    ),
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz("UTC"),  # UTC-aware
        False,
        pendulum.DateTime(2021, 1, 1, 12, 0, 0),  # should become naive (stripped tz)
        "UTC-aware datetime with timezone=False",
    ),
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz(
            timezone(timedelta(hours=5))  # type: ignore[arg-type]
        ),  # non-UTC timezone
        False,
        pendulum.DateTime(2021, 1, 1, 7, 0, 0),  # should convert to UTC time then strip tz
        "non-UTC timezone with timezone=False",
    ),
    (
        pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz(
            timezone(timedelta(hours=-8))  # type: ignore[arg-type]
        ),  # negative offset
        False,
        pendulum.DateTime(2021, 1, 1, 20, 0, 0),  # should convert to UTC time then strip tz
        "negative timezone offset with timezone=False",
    ),
]


@pytest.mark.parametrize("local_tz,exp_dt_tz", zip(LOCAL_TIMEZONES, (None, "UTC", "CET", "IST")))
def test_set_local_tz(local_tz: str, exp_dt_tz: str) -> None:
    dt_tz = time.tzname
    p_tz = pendulum.now().timezone_name
    with local_timezone(local_tz):
        if local_tz:
            assert time.tzname[0] == exp_dt_tz
            assert pendulum.now().timezone_name == local_tz
        else:
            assert dt_tz == time.tzname
            assert p_tz == pendulum.now().timezone_name


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "input_dt, timezone_param, expected, description", normalize_timezone_test_params
)
def test_normalize_timezone(
    local_tz: str,
    input_dt: pendulum.DateTime,
    timezone_param: bool,
    expected: pendulum.DateTime,
    description: str,
) -> None:
    """Test normalize_timezone function with various timezone scenarios."""
    with local_timezone(local_tz):
        result = normalize_timezone(input_dt, timezone_param)

        # Check the datetime value is correct
        assert result == expected, f"Failed for {description}: expected {expected}, got {result}"

        # Check timezone awareness based on the timezone parameter
        if timezone_param:
            # when timezone=True the result is aware in the configured timezone, UTC here
            assert (
                result.tzinfo is not None
            ), f"Failed for {description}: expected timezone-aware datetime"
            assert result.utcoffset() == timedelta(
                0
            ), f"Failed for {description}: expected a UTC offset, got {result.utcoffset()}"
        else:
            # When timezone=False, result should always be naive
            assert (
                result.tzinfo is None
            ), f"Failed for {description}: expected naive datetime, got timezone-aware"

        assert isinstance(
            result, datetime
        ), f"Failed for {description}: expected a datetime, got {type(result)}"


_NAIVE = pendulum.DateTime(2024, 1, 15, 23, 30)
_AWARE = pendulum.DateTime(2024, 1, 15, 23, 30, tzinfo=UTC)
# the same instant, spelled in a named zone and in a nameless fixed offset
_BERLIN = pendulum.DateTime(2024, 1, 16, 0, 30, tzinfo=pendulum.timezone("Europe/Berlin"))
_OFFSET = pendulum.DateTime(2024, 1, 16, 1, 30, tzinfo=fixed_timezone(7200))

# every input x hint pair: wall clock and offset the value must end up with
NORMALIZE_MATRIX = [
    ("UTC", True, _NAIVE, datetime(2024, 1, 15, 23, 30), timedelta(0)),
    ("UTC", True, _AWARE, datetime(2024, 1, 15, 23, 30), timedelta(0)),
    ("UTC", True, _OFFSET, datetime(2024, 1, 15, 23, 30), timedelta(0)),
    ("UTC", False, _AWARE, datetime(2024, 1, 15, 23, 30), None),
    ("UTC", False, _OFFSET, datetime(2024, 1, 15, 23, 30), None),
    ("UTC", False, _NAIVE, datetime(2024, 1, 15, 23, 30), None),
    # a naive value keeps its wall clock, so the instant moves
    ("Europe/Berlin", True, _NAIVE, datetime(2024, 1, 15, 23, 30), timedelta(hours=1)),
    # an aware value keeps its instant, so the wall clock moves
    ("Europe/Berlin", True, _AWARE, datetime(2024, 1, 16, 0, 30), timedelta(hours=1)),
    ("Europe/Berlin", True, _BERLIN, datetime(2024, 1, 16, 0, 30), timedelta(hours=1)),
    ("Europe/Berlin", False, _AWARE, datetime(2024, 1, 16, 0, 30), None),
    ("Europe/Berlin", False, _NAIVE, datetime(2024, 1, 15, 23, 30), None),
]


@pytest.mark.parametrize("via", ["configured", "explicit"])
@pytest.mark.parametrize("tz_name,hint,value,expected_wall_clock,expected_offset", NORMALIZE_MATRIX)
def test_normalize_timezone_matrix(
    via: str,
    tz_name: str,
    hint: bool,
    value: pendulum.DateTime,
    expected_wall_clock: datetime,
    expected_offset: timedelta,
) -> None:
    """`normalize_timezone` puts a value in the configured timezone or in an explicit one."""
    tz = ZoneInfo(tz_name)
    if via == "explicit":
        result = normalize_timezone(value, hint, tz)
    else:
        previous = set_context_timezone(tz)
        try:
            result = normalize_timezone(value, hint)
        finally:
            set_context_timezone(previous)

    assert result.replace(tzinfo=None) == expected_wall_clock
    assert result.utcoffset() == expected_offset
    # an aware input keeps its instant whenever the result is aware
    if value.tzinfo is not None and expected_offset is not None:
        assert result == value
    # a value already in the target zone passes through untouched, whatever tzinfo class it carries
    if hint and value.tzinfo is not None and to_iana_name(value.tzinfo) == tz_name:
        assert result is value
    # the default context is UTC, so the plain call must agree with both UTC forms
    if tz_name == "UTC":
        assert result == normalize_timezone(value, hint)


def test_normalize_timezone_nameless_zones_never_match() -> None:
    """Two fixed offsets carry no name, so even an equal offset goes through the conversion."""
    target = timezone(timedelta(hours=2))
    result = normalize_timezone(_OFFSET, True, target)
    assert result is not _OFFSET
    assert result.tzinfo is target
    assert result == _OFFSET


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
def test_normalize_timezone_edge_cases(local_tz: str) -> None:
    """Test edge cases for normalize_timezone function."""

    with local_timezone(local_tz):
        # Test with microseconds preservation
        dt_with_microseconds = pendulum.DateTime(2021, 1, 1, 12, 30, 45, 123456).in_tz(
            timezone(timedelta(hours=3))  # type: ignore[arg-type]
        )

        # timezone=True should preserve microseconds
        result_true = normalize_timezone(dt_with_microseconds, True)
        assert result_true.microsecond == 123456
        assert result_true == pendulum.DateTime(2021, 1, 1, 9, 30, 45, 123456).in_tz("UTC")

        # timezone=False should preserve microseconds
        result_false = normalize_timezone(dt_with_microseconds, False)
        assert result_false.microsecond == 123456
        assert result_false == pendulum.DateTime(2021, 1, 1, 9, 30, 45, 123456)
        assert result_false.tzinfo is None

        # Test that naive datetime is treated as UTC (system timezone ignored)
        naive_dt = pendulum.DateTime(2021, 1, 1, 12, 0, 0)

        # timezone=True: naive datetime should be treated as UTC
        result_naive_true = normalize_timezone(naive_dt, True)
        assert result_naive_true.utcoffset() == timedelta(0)
        assert result_naive_true == pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz("UTC")

        # timezone=False: naive datetime should remain naive
        result_naive_false = normalize_timezone(naive_dt, False)
        assert result_naive_false.tzinfo is None
        assert result_naive_false == naive_dt


# tests for ensure_pendulum_time
@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(
            # python naive time remains unchanged and naive
            dt_time(12, 34, 56, 123456),
            pendulum.time(12, 34, 56, 123456),
            id="python-naive-time",
        ),
        pytest.param(
            # pendulum naive time remains unchanged and naive
            pendulum.time(6, 7, 8, 9001),
            pendulum.time(6, 7, 8, 9001),
            id="pendulum-naive-time",
        ),
    ],
)
def test_ensure_pendulum_time_naive(local_tz: str, value, expected) -> None:
    """ensure naive times are returned unchanged and naive."""
    with local_timezone(local_tz):
        t = ensure_pendulum_time(value)
        assert (t.hour, t.minute, t.second, t.microsecond) == (
            expected.hour,
            expected.minute,
            expected.second,
            expected.microsecond,
        )
        assert t.tzinfo is None


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "value, expected, case_id",
    [
        (
            # 01:00 at +02:00 equals 23:00 UTC of previous day -> returns 23:00 as naive time
            dt_time(1, 0, 0, tzinfo=timezone(timedelta(hours=2))),
            pendulum.time(23, 0, 0),
            "aware-+02h",
        ),
        (
            # 23:30 at -02:00 equals 01:30 UTC next day
            dt_time(23, 30, 0, tzinfo=timezone(timedelta(hours=-2))),
            pendulum.time(1, 30, 0),
            "aware--02h",
        ),
        (
            # include microseconds
            dt_time(12, 0, 0, 123456, tzinfo=timezone(timedelta(hours=3))),
            pendulum.time(9, 0, 0, 123456),
            "aware-with-microseconds",
        ),
    ],
    ids=lambda p: p if isinstance(p, str) else None,
)
def test_ensure_pendulum_time_aware(local_tz: str, value, expected, case_id: str) -> None:
    """ensure aware times are converted to UTC then made naive."""
    with local_timezone(local_tz):
        t = ensure_pendulum_time(value)
        assert (t.hour, t.minute, t.second, t.microsecond) == (
            expected.hour,
            expected.minute,
            expected.second,
            expected.microsecond,
        )
        assert t.tzinfo is None


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "value, expected, case_id",
    [
        ("12:34:56", pendulum.time(12, 34, 56), "iso-naive"),
        ("01:00:00+02:00", pendulum.time(23, 0, 0), "iso-aware-+02h"),
        ("23:30:00-02:00", pendulum.time(1, 30, 0), "iso-aware--02h"),
        (
            "12:34:56.123456+02:00",
            pendulum.time(10, 34, 56, 123456),
            "iso-aware-fractional",
        ),
    ],
    ids=lambda p: p if isinstance(p, str) else None,
)
def test_ensure_pendulum_time_from_strings(
    local_tz: str, value: str, expected, case_id: str
) -> None:
    """ensure ISO time strings are parsed and normalized to UTC-naive time-of-day."""
    if case_id != "iso-naive":
        pytest.importorskip("pendulum", "3", "pendulum < 3 can't parse time with tz")
    with local_timezone(local_tz):
        t = ensure_pendulum_time(value)
        assert (t.hour, t.minute, t.second, t.microsecond) == (
            expected.hour,
            expected.minute,
            expected.second,
            expected.microsecond,
        )
        assert t.tzinfo is None


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
@pytest.mark.parametrize(
    "value, expected, case_id",
    [
        (timedelta(seconds=0), pendulum.time(0, 0, 0), "td-midnight"),
        (timedelta(seconds=3661, microseconds=2345), pendulum.time(1, 1, 1, 2345), "td-1h1m1s-us"),
        (timedelta(days=1, seconds=1), pendulum.time(0, 0, 1), "td-ignores-days"),
    ],
    ids=lambda p: p if isinstance(p, str) else None,
)
def test_ensure_pendulum_time_from_timedelta(
    local_tz: str, value: timedelta, expected, case_id: str
) -> None:
    """ensure timedelta is treated as seconds since midnight (days ignored)."""
    with local_timezone(local_tz):
        t = ensure_pendulum_time(value)
        assert (t.hour, t.minute, t.second, t.microsecond) == (
            expected.hour,
            expected.minute,
            expected.second,
            expected.microsecond,
        )
        assert t.tzinfo is None


@pytest.mark.parametrize(
    "value",
    [
        "2021-01-01T00:00:00",  # datetime string is not accepted
        "not-a-time",
        3600,  # numeric types are not supported
        3600.0,
    ],
    ids=[
        "str-datetime-not-allowed",
        "invalid-str",
        "int-not-supported",
        "float-not-supported",
    ],
)
def test_ensure_pendulum_time_invalid(value) -> None:
    """ensure invalid inputs raise appropriate errors."""
    if isinstance(value, str):
        with pytest.raises(ValueError):
            ensure_pendulum_time(value)
    else:
        with pytest.raises(TypeError):
            ensure_pendulum_time(value)


@pytest.mark.parametrize(
    "value, expected_offset_hours",
    [
        (datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5))), 5),
        (datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-8))), -8),
        (datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 0),
        (datetime(2021, 6, 15, 12, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))), 5.5),
    ],
    ids=["plus5", "minus8", "utc", "plus5:30"],
)
def test_ensure_datetime_preserves_fixed_offset(value, expected_offset_hours) -> None:
    """A fixed offset survives coercion, including the half-hour one."""
    result = ensure_datetime(value)
    assert result.tzinfo is not None
    assert result.tzinfo.utcoffset(result) == timedelta(hours=expected_offset_hours)


@pytest.mark.parametrize(
    "offset_minutes,expected_offset_hours",
    [(-480, -8), (330, 5.5), (0, 0)],
    ids=["minus8", "plus5:30", "utc"],
)
def test_ensure_pendulum_datetime_nameless_fixed_offset(
    offset_minutes: int, expected_offset_hours: float
) -> None:
    """`pytz.FixedOffset`, which the snowflake connector returns, has neither a name nor a zone."""
    import pytz

    value = datetime(2021, 1, 1, 12, 0, 0, tzinfo=pytz.FixedOffset(offset_minutes))
    # `ensure_pendulum_dt` keeps the original zone, so the offset goes through `to_pendulum_tz`
    result = ensure_pendulum_dt(value)
    assert result.tzinfo.utcoffset(result) == timedelta(hours=expected_offset_hours)
    assert ensure_pendulum_datetime(value, timezone.utc) == result


@pytest.mark.parametrize(
    "value",
    [
        datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5))),
        datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
        datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    ],
    ids=["plus5", "minus8", "utc"],
)
def test_ensure_datetime_add_preserves_tz(value) -> None:
    """timedelta arithmetic must keep the original tz offset."""
    result = ensure_datetime(value)
    original_offset = result.tzinfo.utcoffset(result)

    # python timedelta
    td_added = result + timedelta(days=1, hours=2)
    assert td_added.tzinfo is not None, "timedelta addition produced naive datetime"
    assert td_added.tzinfo.utcoffset(td_added) == original_offset


@pytest.mark.parametrize(
    "value",
    [
        datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5))),
        datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
        datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    ],
    ids=["plus5", "minus8", "utc"],
)
def test_ensure_datetime_add_then_format(value) -> None:
    """after adding a timedelta, datetime_obj_to_str with %:z must not raise on tz formatting."""
    added = ensure_datetime(value) + timedelta(hours=1)
    # must not raise ValueError about missing timezone
    formatted = datetime_obj_to_str(added, "%Y-%m-%dT%H:%M:%S%:z")
    assert "+" in formatted or "-" in formatted


@pytest.mark.parametrize(
    "value, expected_hour",
    [
        (datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5))), 7),
        (datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=-8))), 20),
        (datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc), 12),
    ],
    ids=["plus5", "minus8", "utc"],
)
def test_ensure_pendulum_datetime_add_preserves_utc(value, expected_hour) -> None:
    """UTC conversion must shift the hour correctly and survive .add() arithmetic."""
    result = ensure_pendulum_datetime(value)
    assert result.hour == expected_hour
    assert result.tz == UTC
    assert result.add(days=1).tz == UTC


@pytest.mark.parametrize(
    "value, expected_date",
    [
        # +8h: 2021-01-01T00:00+08:00 = 2020-12-31T16:00 UTC
        (
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8))),
            pendulum.date(2020, 12, 31),
        ),
        # -8h: 2021-01-01T00:00-08:00 = 2021-01-01T08:00 UTC
        (
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
            pendulum.date(2021, 1, 1),
        ),
        (
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            pendulum.date(2021, 1, 1),
        ),
    ],
    ids=["plus8-crosses-date", "minus8-same-date", "utc"],
)
def test_ensure_pendulum_date_stdlib_timezone(value, expected_date) -> None:
    """stdlib timezone offsets crossing midnight must shift the date accordingly."""
    result = ensure_pendulum_date(value)
    assert result == expected_date


@pytest.mark.parametrize(
    "clock_cls",
    [MonotonicPreciseTime, LockedMonotonicPreciseTime],
    ids=["unlocked", "locked"],
)
def test_monotonic_precise_time_never_goes_backward(clock_cls: type) -> None:
    clock = clock_cls()
    values = [clock() for _ in range(10_000)]
    for i in range(1, len(values)):
        assert values[i] >= values[i - 1], f"went backward at index {i}"


@pytest.mark.parametrize(
    "clock_cls",
    [MonotonicPreciseTime, LockedMonotonicPreciseTime],
    ids=["unlocked", "locked"],
)
def test_monotonic_precise_time_tracks_wall_clock(clock_cls: type) -> None:
    clock = clock_cls()
    m = clock()
    w = precise_time()
    # should be within 100ms of wall clock
    assert abs(m - w) < 0.1, f"monotonic {m} too far from wall {w}"


@pytest.mark.parametrize(
    "clock_cls",
    [MonotonicPreciseTime, LockedMonotonicPreciseTime],
    ids=["unlocked", "locked"],
)
def test_monotonic_precise_time_survives_backward_step(clock_cls: type) -> None:
    """Simulate a wall-clock backward jump and verify the high-water mark holds."""
    clock = clock_cls()
    t0 = clock()

    # simulate clock jumping backward by 5 seconds
    with mock.patch("dlt.common.time.precise_time", return_value=t0 - 5.0):
        t1 = clock()
    assert t1 == t0, "clock must return high-water mark when wall clock goes backward"

    # after the backward step, a normal reading must still be >= high-water mark
    t2 = clock()
    assert t2 >= t0


@pytest.mark.parametrize(
    "clock_cls",
    [MonotonicPreciseTime, LockedMonotonicPreciseTime],
    ids=["unlocked", "locked"],
)
def test_strictly_increasing_advances_on_frozen_clock(clock_cls: type) -> None:
    """With strictly_increasing=True, every call returns a strictly greater value."""
    clock = clock_cls(strictly_increasing=True)
    t0 = clock()

    # freeze wall clock at current value
    with mock.patch("dlt.common.time.precise_time", return_value=t0):
        values = [clock() for _ in range(50)]

    for i in range(1, len(values)):
        assert values[i] > values[i - 1], f"not strictly increasing at index {i}"

    # values should still be very close to wall clock (nextafter increments)
    assert values[-1] - t0 < 0.001


def test_increasing_precise_time_is_strictly_increasing() -> None:
    """Module-level increasing_precise_time must produce unique values."""
    assert isinstance(increasing_precise_time, LockedMonotonicPreciseTime)
    values = [increasing_precise_time() for _ in range(100)]
    for i in range(1, len(values)):
        assert values[i] > values[i - 1], f"not strictly increasing at index {i}"


def test_create_load_id_strictly_increasing() -> None:
    """create_load_id must return strictly increasing values even under clock jitter."""
    ids = [create_load_id() for _ in range(100)]
    for i in range(1, len(ids)):
        assert float(ids[i]) > float(
            ids[i - 1]
        ), f"load id not strictly increasing at index {i}: {ids[i - 1]} >= {ids[i]}"

    # simulate a backward jump: mock the module-level singleton
    baseline = float(create_load_id())
    with mock.patch(
        "dlt.common.storages.load_package.increasing_precise_time",
        return_value=baseline - 10.0,
    ):
        backward_id = float(create_load_id())
    assert backward_id == baseline - 10.0  # mock fully replaces the callable

    # after removing the mock the real singleton still has its high-water mark
    restored = float(create_load_id())
    assert restored >= baseline


def test_date_to_epoch_days() -> None:
    assert date_to_epoch_days(date(1970, 1, 1)) == 0
    assert date_to_epoch_days(pendulum.date(1970, 1, 2)) == 1


@pytest.mark.parametrize(
    "fn, args",
    [
        (ensure_datetime, ("2021-01-01T12:00:00+02:00",)),
        (ensure_datetime_in_tz, ("2021-01-01T12:00:00+02:00",)),
        (ensure_datetime_in_tz, ("2021-01-01T12:00:00+02:00", ZoneInfo("Europe/Berlin"))),
    ],
    ids=["ensure_datetime", "ensure_datetime_in_tz_default", "ensure_datetime_in_tz_explicit"],
)
def test_ensure_datetime_helpers_return_stdlib(fn, args) -> None:
    """All three helpers return stdlib `datetime.datetime`, not `pendulum.DateTime`."""
    result = fn(*args)
    # `type(...) is` not `isinstance(...)`: pendulum.DateTime is a datetime subclass
    assert type(result) is datetime


@pytest.mark.parametrize(
    "value, default_tz, expected",
    [
        # naive + Berlin (UTC+1 in January) → 11:00 UTC
        (
            "2021-01-15T12:00:00",
            ZoneInfo("Europe/Berlin"),
            datetime(2021, 1, 15, 11, tzinfo=timezone.utc),
        ),
        # naive + default_tz omitted → naive treated as UTC (regression guard)
        (
            "2021-01-15T12:00:00",
            None,
            datetime(2021, 1, 15, 12, tzinfo=timezone.utc),
        ),
        # aware input ignores default_tz: only the input's own +02:00 offset matters
        (
            datetime(2021, 1, 15, 12, tzinfo=timezone(timedelta(hours=2))),
            ZoneInfo("Asia/Tokyo"),
            datetime(2021, 1, 15, 10, tzinfo=timezone.utc),
        ),
    ],
    ids=["naive-with-berlin", "naive-default-utc", "aware-ignores-default"],
)
def test_ensure_datetime_in_tz_interprets_naive_only(value, default_tz, expected: datetime) -> None:
    """`tz` is the interpretation for naive inputs only; aware inputs keep their instant."""
    if default_tz is None:
        result = ensure_datetime_in_tz(value)
    else:
        result = ensure_datetime_in_tz(value, default_tz)
    assert result == expected
    # always lands in the requested zone, the context default being UTC
    assert result.tzinfo == (default_tz or timezone.utc)


@pytest.mark.parametrize(
    "value, tz, expected_wall_clock",
    [
        # naive str → wall-clock preserved, tz attached
        ("2021-01-15T12:00:00", ZoneInfo("Europe/Berlin"), (2021, 1, 15, 12, 0)),
        # naive datetime → same
        (datetime(2021, 1, 15, 12, 0), ZoneInfo("Asia/Kolkata"), (2021, 1, 15, 12, 0)),
        # aware utc → converted to Berlin (UTC+1 in January)
        (
            datetime(2021, 1, 15, 12, tzinfo=timezone.utc),
            ZoneInfo("Europe/Berlin"),
            (2021, 1, 15, 13, 0),
        ),
        # aware string +05:00 → converted to Tokyo (+09:00) → +4h
        ("2021-01-15T12:00:00+05:00", ZoneInfo("Asia/Tokyo"), (2021, 1, 15, 16, 0)),
        # already in the zone under pendulum's tzinfo, the requested object is still pinned
        (pendulum.DateTime(2021, 1, 15, 12, tzinfo=UTC), timezone.utc, (2021, 1, 15, 12, 0)),
    ],
    ids=[
        "naive-str-attach",
        "naive-datetime-attach",
        "aware-convert-to-berlin",
        "aware-string-convert-to-tokyo",
        "pendulum-utc-pinned",
    ],
)
def test_ensure_datetime_in_tz(value, tz, expected_wall_clock) -> None:
    """Naive inputs get `tz` attached; aware inputs are converted to `tz`."""
    result = ensure_datetime_in_tz(value, tz)
    assert result.tzinfo is tz
    assert (
        result.year,
        result.month,
        result.day,
        result.hour,
        result.minute,
    ) == expected_wall_clock


def test_ensure_datetime_preserves_tz_and_naive() -> None:
    """Sanity: `ensure_datetime` keeps original tz and leaves naive inputs naive."""
    aware = ensure_datetime("2021-01-15T12:00:00+05:00")
    assert aware.tzinfo is not None
    assert aware.utcoffset() == timedelta(hours=5)

    naive = ensure_datetime("2021-01-15T12:00:00")
    assert naive.tzinfo is None


@pytest.mark.parametrize(
    "zone_name,moment,offset_hours",
    [
        ("Europe/Berlin", datetime(2024, 1, 15, 23, 30), 1),
        ("Europe/Berlin", datetime(2024, 7, 15, 23, 30), 2),
        ("Asia/Kolkata", datetime(2024, 1, 15, 23, 30), 5.5),
        ("UTC", datetime(2024, 1, 15, 23, 30), 0),
    ],
)
def test_zoneinfo_keeps_its_offset(zone_name: str, moment: datetime, offset_hours: float) -> None:
    """A `ZoneInfo` must survive the pendulum round-trip: pendulum 2 resolves one only by name."""
    utc_moment = moment.replace(tzinfo=timezone.utc)
    value = utc_moment.astimezone(ZoneInfo(zone_name))
    assert value.utcoffset() == timedelta(hours=offset_hours)

    # the instant is what must not move, whichever way it is spelled
    assert ensure_datetime(value).utcoffset() == timedelta(hours=offset_hours)
    assert ensure_datetime_in_tz(value) == utc_moment
    assert datetime_to_timestamp(value) == int(utc_moment.timestamp())
    assert to_pendulum_tz(ZoneInfo(zone_name)).utcoffset(moment) == timedelta(hours=offset_hours)
