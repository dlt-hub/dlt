import datetime  # noqa: I251
from typing import Optional, cast
from zoneinfo import ZoneInfo

import pytest
from pendulum.tz import UTC as PENDULUM_UTC, fixed_timezone

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import TimezoneContext
from dlt.common.time import (
    InvalidTimezoneName,
    get_context_timezone,
    set_context_timezone,
    to_iana_name,
)


def test_default_is_utc() -> None:
    assert get_context_timezone() == datetime.timezone.utc
    assert Container()[TimezoneContext].timezone == "UTC"


@pytest.mark.parametrize(
    "name", ["utc", "+02:00", "-05:00", "Nowhere/Bogus", "Europe/Berlin/Extra"]
)
def test_rejects_names_arrow_cannot_use(name: str) -> None:
    """A name must resolve in `zoneinfo`, which is what `pyarrow` accepts too."""
    with pytest.raises(InvalidTimezoneName) as exc:
        TimezoneContext(name).tzinfo
    assert exc.value.timezone == name
    assert "canonical IANA name" in str(exc.value)


@pytest.mark.parametrize(
    "tz,expected",
    [
        (ZoneInfo("Europe/Berlin"), "Europe/Berlin"),
        (datetime.timezone.utc, "UTC"),
        (PENDULUM_UTC, "UTC"),
        (datetime.timezone(datetime.timedelta(hours=2)), None),
        (fixed_timezone(7200), None),
        (None, None),
    ],
)
def test_to_iana_name(tz: Optional[datetime.tzinfo], expected: Optional[str]) -> None:
    """Only a zone that carries a name can be installed; a fixed offset cannot."""
    assert to_iana_name(tz) == expected


def test_installs_and_unwinds() -> None:
    container = Container()
    with container.injectable_context(TimezoneContext("Europe/Berlin")):
        assert get_context_timezone() == ZoneInfo("Europe/Berlin")
        with container.injectable_context(TimezoneContext("Asia/Kolkata")):
            assert get_context_timezone() == ZoneInfo("Asia/Kolkata")
        assert get_context_timezone() == ZoneInfo("Europe/Berlin")
    assert get_context_timezone() == datetime.timezone.utc


def test_passed_to_worker_processes() -> None:
    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        worker_contexts = Container().get_worker_contexts()
    passed = cast(TimezoneContext, worker_contexts[TimezoneContext])
    assert passed.timezone == "Europe/Berlin"


def test_set_configured_timezone_returns_previous() -> None:
    berlin = ZoneInfo("Europe/Berlin")
    previous = set_context_timezone(berlin)
    try:
        assert previous == datetime.timezone.utc
        assert get_context_timezone() == berlin
        # `None` resets to UTC
        assert set_context_timezone(None) == berlin
        assert get_context_timezone() == datetime.timezone.utc
    finally:
        set_context_timezone(previous)
