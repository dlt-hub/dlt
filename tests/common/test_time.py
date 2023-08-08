import pytest
from datetime import datetime, date, timezone, timedelta  # noqa: I251
from pendulum.tz import UTC

from dlt.common import pendulum
from dlt.common.time import timestamp_before, timestamp_within, ensure_pendulum_datetime, ensure_pendulum_date
from dlt.common.typing import TAnyDateTime


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
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # python datetime with tz
    (
        datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
        pendulum.datetime(2021, 1, 1, 8, 0, 0).in_tz("UTC"),
    ),
    # python date object
    (date(2021, 1, 1), pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
    # pendulum datetime with tz
    (
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # pendulum datetime without tz
    (
        pendulum.datetime(2021, 1, 1, 0, 0, 0),
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # iso datetime in UTC
    ("2021-01-01T00:00:00+00:00", pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
    # iso datetime with non utc tz
    (
        "2021-01-01T00:00:00+05:00",
        pendulum.datetime(2021, 1, 1, 0, 0, 0, tz=5),
    ),
    # iso datetime without tz
    (
        "2021-01-01T05:02:32",
        pendulum.datetime(2021, 1, 1, 5, 2, 32).in_tz("UTC"),
    ),
    # iso date
    ("2021-01-01", pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
]


@pytest.mark.parametrize("date_value, expected", test_params)
def test_ensure_pendulum_datetime(
    date_value: TAnyDateTime, expected: pendulum.DateTime
) -> None:
    dt = ensure_pendulum_datetime(date_value)
    assert dt == expected
    # always UTC
    assert dt.tz == UTC
    # always pendulum
    assert isinstance(dt, pendulum.DateTime)


def test_ensure_pendulum_date_utc() -> None:
    # when converting from datetimes make sure to shift to UTC before doing date
    assert ensure_pendulum_date("2021-01-01T00:00:00+05:00") == pendulum.date(2020, 12, 31)
    assert ensure_pendulum_date(datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))) == pendulum.date(2020, 12, 31)