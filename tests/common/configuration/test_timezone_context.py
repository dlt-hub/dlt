import contextlib
import datetime  # noqa: I251
from typing import Dict, Iterator, Optional, cast
from zoneinfo import ZoneInfo

import pytest
from pendulum.tz import UTC as PENDULUM_UTC, fixed_timezone, local_timezone

import dlt
from dlt.common import known_env, pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import InvalidTimezoneName, TimezoneContext
from dlt.common.time import to_iana_name
from dlt.common.time import (
    get_context_timezone,
    normalize_timezone,
    set_context_timezone,
)
from dlt.common.typing import TTimeInterval
from dlt.extract.incremental.context import TimeIntervalContext

from tests.common.configuration.utils import environment


@pytest.fixture(autouse=True)
def restore_timezone() -> Iterator[None]:
    """`preserve_run_context` is autouse only under `tests/workspace`, so undo the context here."""
    container = Container()
    had_context = TimezoneContext in container
    previous = get_context_timezone()
    yield
    if not had_context:
        with contextlib.suppress(KeyError):
            del container[TimezoneContext]
    set_context_timezone(previous)


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


def test_detects_from_interval_env(environment: Dict[str, str]) -> None:
    """A spawned interpreter inherits only env; the interval carries the zone, not a second var."""
    environment[known_env.DLT_INTERVAL_START] = "2024-01-15T00:00:00+00:00"
    environment[known_env.DLT_INTERVAL_END] = "2024-01-16T00:00:00+00:00"
    environment[known_env.DLT_INTERVAL_TIMEZONE] = "Asia/Kolkata"
    with Container().injectable_context(TimeIntervalContext()):
        assert dlt.current.timezone() == ZoneInfo("Asia/Kolkata")


def test_passed_to_worker_processes() -> None:
    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        worker_contexts = Container().get_worker_contexts()
    passed = cast(TimezoneContext, worker_contexts[TimezoneContext])
    assert passed.timezone == "Europe/Berlin"


def _interval(tz: Optional[datetime.tzinfo]) -> TTimeInterval:
    return TTimeInterval(
        datetime.datetime(2024, 1, 15, tzinfo=tz), datetime.datetime(2024, 1, 16, tzinfo=tz)
    )


def test_interval_installs_its_timezone() -> None:
    berlin, kolkata = ZoneInfo("Europe/Berlin"), ZoneInfo("Asia/Kolkata")
    container = Container()
    with container.injectable_context(TimeIntervalContext(interval=_interval(berlin))):
        assert dlt.current.timezone() == berlin
        # replacing the interval moves the timezone with it
        dlt.current.interval.set(_interval(kolkata))
        assert dlt.current.timezone() == kolkata
    # the timezone belongs to the run, so it outlives the interval context that installed it
    assert dlt.current.timezone() == kolkata
    assert container[TimezoneContext].timezone == "Asia/Kolkata"


@pytest.mark.parametrize(
    "tz", [None, datetime.timezone(datetime.timedelta(hours=2))], ids=["naive", "fixed-offset"]
)
def test_interval_without_a_name_keeps_utc(tz: Optional[datetime.tzinfo]) -> None:
    """Neither a naive interval nor a fixed offset names a zone dlt could store values in."""
    with Container().injectable_context(TimeIntervalContext(interval=_interval(tz))):
        assert dlt.current.timezone() == datetime.timezone.utc
    assert dlt.current.timezone() == datetime.timezone.utc


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


def test_does_not_move_pendulum_local_timezone() -> None:
    """The timezone must never be implemented by moving pendulum's local zone.

    Asserts the value is unchanged rather than UTC: other tests set the local zone on purpose.
    """
    before = local_timezone()
    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        assert local_timezone() == before
        assert pendulum.now().tzinfo == before
    assert local_timezone() == before


@pytest.mark.parametrize("hint", [True, False], ids=["tz_aware", "tz_naive"])
def test_pendulum_utc_value_is_recognized(hint: bool) -> None:
    """A value carrying pendulum's own UTC must be treated as UTC, not as an unknown zone."""
    value = pendulum.DateTime(2024, 1, 15, 23, 30, tzinfo=PENDULUM_UTC)
    assert to_iana_name(value.tzinfo) == "UTC"

    # with a UTC context the instant is untouched, and `timezone=False` only strips the zone
    normalized = normalize_timezone(value, hint)
    if hint:
        assert normalized == value
        assert normalized.utcoffset() == datetime.timedelta(0)
    else:
        assert normalized == value.replace(tzinfo=None)
        assert normalized.tzinfo is None

    # with a Berlin context the instant survives and the offset moves
    with Container().injectable_context(TimezoneContext("Europe/Berlin")):
        shifted = normalize_timezone(value, hint)
    assert shifted.replace(tzinfo=None) == datetime.datetime(2024, 1, 16, 0, 30)
    if hint:
        assert shifted == value
        assert shifted.utcoffset() == datetime.timedelta(hours=1)
    else:
        assert shifted.tzinfo is None


def test_pendulum_utc_interval_installs_utc() -> None:
    """An interval whose bounds carry pendulum UTC resolves to UTC, not to "no IANA name"."""
    interval = TTimeInterval(
        pendulum.DateTime(2024, 1, 15, tzinfo=PENDULUM_UTC),
        pendulum.DateTime(2024, 1, 16, tzinfo=PENDULUM_UTC),
    )
    with Container().injectable_context(TimeIntervalContext(interval=interval)):
        assert Container()[TimezoneContext].timezone == "UTC"
        assert dlt.current.timezone() == datetime.timezone.utc
