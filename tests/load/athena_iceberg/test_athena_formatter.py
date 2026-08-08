from datetime import datetime, date, timezone  # noqa: I251
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from dlt.common import pendulum
from dlt.destinations.impl.athena.sql_client import DLTAthenaFormatter

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def _format(value: Any) -> str:
    return DLTAthenaFormatter().format("%(v)s", {"v": value})


@pytest.mark.parametrize(
    "value",
    [
        datetime(2024, 3, 4, 4, 6, 7, 123456, tzinfo=timezone.utc),
        datetime(2024, 3, 4, 5, 6, 7, 123456, tzinfo=ZoneInfo("Europe/Berlin")),
        datetime(2024, 3, 4, 9, 36, 7, 123456, tzinfo=ZoneInfo("Asia/Kolkata")),
        pendulum.datetime(2024, 3, 4, 5, 6, 7, 123456, tz="Europe/Berlin"),
        # naive is UTC by dlt convention, so it renders as written
        datetime(2024, 3, 4, 4, 6, 7, 123456),
    ],
)
def test_datetime_param_is_utc(value: datetime) -> None:
    """Athena has no timezone type, so a parameter must be converted to UTC before the offset goes."""
    assert _format(value) == "TIMESTAMP '2024-03-04 04:06:07.123456'"


def test_date_param() -> None:
    assert _format(date(2024, 3, 4)) == "DATE '2024-03-04'"
    assert _format(pendulum.date(2024, 3, 4)) == "DATE '2024-03-04'"
