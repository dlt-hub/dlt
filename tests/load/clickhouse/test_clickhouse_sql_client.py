import datetime

import pendulum
import pytest

from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient

from tests.load.clickhouse.utils import clickhouse_client


@pytest.mark.parametrize(
    "case",
    [
        # (query, params, expected result, id)
        # without params
        ("SELECT 16 % 4", (), 0, "%-as-modulo"),
        ("SELECT 'test' LIKE '%es%'", (), 1, "%-as-wildcard"),
        ("SELECT '100% sure'", (), "100% sure", "%-as-literal"),
        # with params
        ("SELECT %s AS value", ("foo",), "foo", "simple-param-val"),
        ("SELECT %s AS value", ("foo%",), "foo%", "%-in-param-val"),
    ],
    ids=lambda case: case[3],
)
def test_clickhouse_execute_query_with_pct(clickhouse_client: ClickHouseClient, case) -> None:
    query, params, expected, _ = case
    with clickhouse_client.sql_client as client:
        with client.execute_query(query, params) as cursor:
            result = cursor.fetchall()
            assert result == [(expected,)]


@pytest.mark.parametrize(
    "input_dt,expected_utc_naive",
    [
        # pendulum with UTC timezone
        (
            pendulum.datetime(2024, 6, 26, 12, 34, 56, 123456, tz="UTC"),
            datetime.datetime(2024, 6, 26, 12, 34, 56, 123456),
        ),
        # pendulum with non-UTC timezone (US/Eastern is UTC-4 in June)
        (
            pendulum.datetime(2024, 6, 26, 12, 0, 0, 500000, tz="US/Eastern"),
            datetime.datetime(2024, 6, 26, 16, 0, 0, 500000),
        ),
        # python datetime with UTC timezone
        (
            datetime.datetime(2024, 6, 26, 12, 34, 56, 123456, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 6, 26, 12, 34, 56, 123456),
        ),
        # python datetime with non-UTC timezone (UTC+5:30)
        (
            datetime.datetime(
                2024,
                6,
                26,
                18,
                4,
                56,
                654321,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            ),
            datetime.datetime(2024, 6, 26, 12, 34, 56, 654321),
        ),
        # naive python datetime (assumed UTC, passed through as-is)
        (
            datetime.datetime(2024, 6, 26, 12, 34, 56, 789000),
            datetime.datetime(2024, 6, 26, 12, 34, 56, 789000),
        ),
    ],
    ids=[
        "pendulum-utc",
        "pendulum-eastern",
        "python-utc",
        "python-offset",
        "python-naive",
    ],
)
def test_clickhouse_datetime_param_round_trip(
    clickhouse_client: ClickHouseClient,
    input_dt: datetime.datetime,
    expected_utc_naive: datetime.datetime,
) -> None:
    """Datetime query parameters are converted to UTC, preserve microseconds,
    and round-trip correctly through ClickHouse DateTime64(6) columns."""
    with clickhouse_client.sql_client as client:
        with client.execute_query("SELECT toDateTime64(%s, 6, 'UTC') AS dt", input_dt) as cursor:
            row = cursor.fetchone()
            result_dt = row[0]
            # clickhouse-driver returns datetime objects; compare as naive UTC
            if hasattr(result_dt, "replace"):
                result_dt = result_dt.replace(tzinfo=None)
            assert (
                result_dt == expected_utc_naive
            ), f"Expected {expected_utc_naive}, got {result_dt} for input {input_dt}"
