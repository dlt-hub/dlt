from datetime import datetime, date, timezone  # noqa: I251
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from dlt.common import pendulum
from dlt.common.time import set_context_timezone
from dlt.destinations.impl.athena.sql_client import DLTAthenaFormatter

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def _format(value: Any) -> str:
    return DLTAthenaFormatter().format("%(v)s", {"v": value})


@pytest.mark.parametrize(
    "context_tz,expected",
    [
        pytest.param("UTC", "2024-03-04 04:06:07.123456", id="utc-context"),
        pytest.param("Europe/Berlin", "2024-03-04 05:06:07.123456", id="berlin-context"),
    ],
)
@pytest.mark.parametrize(
    "value",
    [
        datetime(2024, 3, 4, 4, 6, 7, 123456, tzinfo=timezone.utc),
        datetime(2024, 3, 4, 5, 6, 7, 123456, tzinfo=ZoneInfo("Europe/Berlin")),
        datetime(2024, 3, 4, 9, 36, 7, 123456, tzinfo=ZoneInfo("Asia/Kolkata")),
        pendulum.datetime(2024, 3, 4, 5, 6, 7, 123456, tz="Europe/Berlin"),
    ],
    ids=["utc", "berlin", "kolkata", "pendulum-berlin"],
)
def test_datetime_param_is_context_wall_clock(
    context_tz: str, expected: str, value: datetime
) -> None:
    """Athena has no timezone type, so a parameter is moved to the context zone before the offset goes."""
    set_context_timezone(ZoneInfo(context_tz))
    assert _format(value) == f"TIMESTAMP '{expected}'"


@pytest.mark.parametrize(
    "context_tz", ["UTC", "Europe/Berlin"], ids=["utc-context", "berlin-context"]
)
def test_naive_datetime_param_renders_as_written(context_tz: str) -> None:
    # a naive value already is the context wall clock
    set_context_timezone(ZoneInfo(context_tz))
    assert (
        _format(datetime(2024, 3, 4, 4, 6, 7, 123456)) == "TIMESTAMP '2024-03-04 04:06:07.123456'"
    )


def test_date_param() -> None:
    assert _format(date(2024, 3, 4)) == "DATE '2024-03-04'"
    assert _format(pendulum.date(2024, 3, 4)) == "DATE '2024-03-04'"
