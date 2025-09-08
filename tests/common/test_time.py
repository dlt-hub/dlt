import pytest
from datetime import datetime, date, timezone, timedelta  # noqa: I251
from pendulum.tz import UTC

from dlt.common import pendulum
from dlt.common.time import (
    parse_iso_like_datetime,
    timestamp_before,
    timestamp_within,
    ensure_pendulum_datetime,
    ensure_pendulum_date,
    datetime_to_timestamp,
    datetime_to_timestamp_ms,
    detect_datetime_format,
    ensure_pendulum_datetime_non_utc,
)
from dlt.common.typing import TAnyDateTime
from dlt.common.time import datatime_obj_to_str


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


test_params = [
    # python datetime without tz
    (
        datetime(2021, 1, 1, 0, 0, 0),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # python datetime with tz
    (
        datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
        pendulum.DateTime(2021, 1, 1, 8, 0, 0).in_tz("UTC"),
    ),
    # python date object
    (date(2021, 1, 1), pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
    # pendulum datetime with tz
    (
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # pendulum datetime without tz
    (
        pendulum.DateTime(2021, 1, 1, 0, 0, 0),
        pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # iso datetime in UTC
    ("2021-01-01T00:00:00+00:00", pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
    # iso datetime with non utc tz
    (
        "2021-01-01T00:00:00+05:00",
        pendulum.datetime(2021, 1, 1, 0, 0, 0, tz=5),
    ),
    # iso datetime without tz
    (
        "2021-01-01T05:02:32",
        pendulum.DateTime(2021, 1, 1, 5, 2, 32).in_tz("UTC"),
    ),
    # iso date
    ("2021-01-01", pendulum.DateTime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
]


def test_parse_iso_like_datetime() -> None:
    # naive datetime is still naive
    assert parse_iso_like_datetime("2021-01-01T05:02:32") == pendulum.DateTime(2021, 1, 1, 5, 2, 32)
    # test that _parse_common form pendulum parsing is not failing with KeyError
    assert parse_iso_like_datetime("2021:01:01 05:02:32") == pendulum.DateTime(2021, 1, 1, 5, 2, 32)


@pytest.mark.parametrize("date_value, expected", test_params)
def test_ensure_pendulum_datetime(date_value: TAnyDateTime, expected: pendulum.DateTime) -> None:
    dt = ensure_pendulum_datetime(date_value)
    assert dt == expected
    # always UTC
    assert dt.tz == UTC
    # always pendulum
    assert isinstance(dt, pendulum.DateTime)


def test_ensure_pendulum_date_utc() -> None:
    # when converting from datetimes make sure to shift to UTC before doing date
    assert ensure_pendulum_date("2021-01-01T00:00:00+05:00") == pendulum.date(2020, 12, 31)
    assert ensure_pendulum_date(
        datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))
    ) == pendulum.date(2020, 12, 31)


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
        ("202410", "%Y%m"),  # Compact year and month format
    ],
)
def test_detect_datetime_format(value, expected_format) -> None:
    assert detect_datetime_format(value) == expected_format
    assert ensure_pendulum_datetime(value) is not None


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
    assert datatime_obj_to_str(datetime, datetime_format) == expected_value


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
