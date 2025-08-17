import pytest
import os
import time
from datetime import datetime, date, timezone, timedelta, time as dt_time  # noqa: I251
from pendulum.tz import UTC, fixed_timezone
from contextlib import contextmanager

from dlt.common import pendulum
from dlt.common.time import (
    parse_iso_like_datetime,
    timestamp_before,
    timestamp_within,
    ensure_pendulum_datetime_utc,
    ensure_pendulum_date,
    datetime_to_timestamp,
    datetime_to_timestamp_ms,
    detect_datetime_format,
    ensure_pendulum_datetime_non_utc,
    ensure_pendulum_time,
    normalize_timezone,
)
from dlt.common.typing import TAnyDateTime
from dlt.common.time import datetime_obj_to_str


@contextmanager
def local_timezone(tz_name: str):
    """Context manager to temporarily set local timezone."""

    # do not change when not set
    if not tz_name:
        yield
        return

    if not hasattr(time, "tzset") or os.name == "nt":
        pytest.skip("Timezone manipulation requires tzset (not available on Windows)")

    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = tz_name
    time.tzset()

    try:
        # pendulum has test utils in core library
        with pendulum.test_local_timezone(pendulum._safe_timezone(tz_name)):  # type: ignore[arg-type]
            yield
    finally:
        if old_tz is None:
            if "TZ" in os.environ:
                del os.environ["TZ"]
        else:
            os.environ["TZ"] = old_tz
        time.tzset()


# Different local timezones to test against
local_timezones = [
    None,  # do not change anything, must run on all oses and python versions
    "UTC",  # Keep existing (assuming tests run in UTC by default)
    "Europe/Berlin",  # Berlin timezone
    "Asia/Kolkata",  # India timezone
]


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


@pytest.mark.parametrize("local_tz", local_timezones)
@pytest.mark.parametrize(
    "date_value, expected_utc, expected_non_utc, expected_date", datetime_test_params
)
def test_ensure_pendulum_datetime_utc(
    local_tz: str,
    date_value: TAnyDateTime,
    expected_utc: pendulum.DateTime,
    expected_non_utc: pendulum.DateTime,
    expected_date: pendulum.Date,
) -> None:
    with local_timezone(local_tz):
        dt = ensure_pendulum_datetime_utc(date_value)
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


@pytest.mark.parametrize("local_tz", local_timezones)
@pytest.mark.parametrize(
    "date_value, expected_utc, expected_non_utc, expected_date", datetime_test_params
)
def test_ensure_pendulum_datetime_non_utc(
    local_tz: str,
    date_value: TAnyDateTime,
    expected_utc: pendulum.DateTime,
    expected_non_utc: pendulum.DateTime,
    expected_date: pendulum.Date,
) -> None:
    with local_timezone(local_tz):
        dt = ensure_pendulum_datetime_non_utc(date_value)
        assert dt == expected_non_utc

        def _test_tz(dt_: pendulum.DateTime) -> None:
            # timezone awareness preserved
            if dt.tzinfo or expected_non_utc.tzinfo:
                assert dt.tzinfo.utcoffset(dt) == expected_non_utc.tzinfo.utcoffset(
                    expected_non_utc
                )
            else:
                assert dt.tz is expected_non_utc.tz is None

        _test_tz(dt)
        # always pendulum
        assert isinstance(dt, pendulum.DateTime)
        # NOTE: pendulum destroys timezone information, here we make sure we don't do that
        # works with timedelta
        dt_add = dt + timedelta(days=1)
        _test_tz(dt_add)
        # works with add()
        _test_tz(dt.add(days=1))


@pytest.mark.parametrize("local_tz", local_timezones)
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


@pytest.mark.parametrize("local_tz", local_timezones)
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
        ("2024-W42", "%Y-W%W"),  # Week-based date
        ("2024-W42-5", "%Y-W%W-%u"),  # Week-based date with day
        ("2024-293", "%Y-%j"),  # Ordinal date
        ("20241020", "%Y%m%d"),  # Compact date format
        # ("202410", "%Y%m"),  # Compact year and month format NOTE: does not pass with pendulum < 3.0.0
    ],
)
def test_detect_datetime_format(value, expected_format) -> None:
    assert detect_datetime_format(value) == expected_format
    assert ensure_pendulum_datetime_utc(value) is not None


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
    datetime = ensure_pendulum_datetime_non_utc(datetime_str)
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
        ensure_pendulum_datetime_utc(value)


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


@pytest.mark.parametrize("local_tz,exp_dt_tz", zip(local_timezones, (None, "UTC", "CET", "IST")))
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


@pytest.mark.parametrize("local_tz", local_timezones)
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
            # When timezone=True, result should always be UTC-aware
            assert (
                result.tz == UTC
            ), f"Failed for {description}: expected UTC timezone, got {result.tz}"
            assert (
                result.tzinfo is not None
            ), f"Failed for {description}: expected timezone-aware datetime"
        else:
            # When timezone=False, result should always be naive
            assert (
                result.tzinfo is None
            ), f"Failed for {description}: expected naive datetime, got timezone-aware"

        # Ensure result is always a pendulum DateTime
        assert isinstance(
            result, pendulum.DateTime
        ), f"Failed for {description}: expected pendulum.DateTime, got {type(result)}"


@pytest.mark.parametrize("local_tz", local_timezones)
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
        assert result_naive_true.tz == UTC
        assert result_naive_true == pendulum.DateTime(2021, 1, 1, 12, 0, 0).in_tz("UTC")

        # timezone=False: naive datetime should remain naive
        result_naive_false = normalize_timezone(naive_dt, False)
        assert result_naive_false.tzinfo is None
        assert result_naive_false == naive_dt


# tests for ensure_pendulum_time
@pytest.mark.parametrize("local_tz", local_timezones)
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


@pytest.mark.parametrize("local_tz", local_timezones)
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


@pytest.mark.parametrize("local_tz", local_timezones)
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


@pytest.mark.parametrize("local_tz", local_timezones)
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
