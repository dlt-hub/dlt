"""Tests for TimeIntervalContext — creation, detection, and dlt.current.interval()."""

import os
import time
from datetime import date, datetime, timezone  # noqa: I251
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import InvalidTimezoneName, TimezoneContext
from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_datetime, ensure_pendulum_datetime
from dlt.common.typing import TTimeInterval
from dlt.common.utils import uniq_id
from dlt.extract.incremental.context import (
    TimeIntervalContext,
    get_interval_context,
    interval as _interval_accessor,
)
from dlt.extract.incremental.exceptions import ExternalSchedulerNotAvailable, JoinSchedulerError

from tests.extract.utils import AssertItems, data_item_to_list
from tests.utils import (
    ALL_TEST_DATA_ITEM_FORMATS,
    LOCAL_TIMEZONES,
    TestDataItemFormat,
    data_to_item_format,
    local_timezone,
)


def _utc_iv(start: str, end: str) -> TTimeInterval:
    """Build a UTC interval from ISO strings."""
    return TTimeInterval(ensure_pendulum_datetime(start), ensure_pendulum_datetime(end))


def test_explicit_context_with_tuple() -> None:
    """Created with (start, end) tuple, .interval returns it."""
    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    assert ctx.interval == iv


def test_explicit_context_with_pendulum() -> None:
    start = pendulum.datetime(2024, 6, 1, tz="UTC")
    end = pendulum.datetime(2024, 6, 2, tz="UTC")
    ctx = TimeIntervalContext(interval=TTimeInterval(start, end))
    assert ctx.interval == (start, end)


def test_explicit_context_with_plain_tuple() -> None:
    """A plain (start, end) tuple is accepted and normalized to TTimeInterval, so
    `.start`/`.end` work the same as when a TTimeInterval is passed."""
    start = pendulum.datetime(2024, 6, 1, tz="UTC")
    end = pendulum.datetime(2024, 6, 2, tz="UTC")

    # constructor accepts a plain tuple
    ctx = TimeIntervalContext(interval=(start, end))
    assert isinstance(ctx.interval, TTimeInterval)
    assert ctx.interval == (start, end)
    assert ctx.interval.start == start
    assert ctx.interval.end == end

    # setter accepts a plain tuple too
    ctx.interval = (start, end)
    assert isinstance(ctx.interval, TTimeInterval)

    # dlt.current.interval.set() normalizes as well
    with Container().injectable_context(TimeIntervalContext()):
        _interval_accessor.set((start, end))
        assert isinstance(_interval_accessor(), TTimeInterval)
        assert _interval_accessor() == (start, end)


def test_no_interval_when_empty() -> None:
    with patch.dict(os.environ, {}, clear=True):
        os.environ.pop("DLT_INTERVAL_START", None)
        os.environ.pop("DLT_INTERVAL_END", None)
        ctx = TimeIntervalContext()
    assert ctx.interval is None


@pytest.mark.parametrize(
    "env_vars,expect_interval",
    [
        # both present → detected
        (
            {
                "DLT_INTERVAL_START": "2024-03-01T00:00:00Z",
                "DLT_INTERVAL_END": "2024-03-02T00:00:00Z",
            },
            True,
        ),
        # start only → partial → None
        ({"DLT_INTERVAL_START": "2024-03-01T00:00:00Z"}, False),
        # neither → None
        ({}, False),
    ],
    ids=["both-present", "start-only-partial", "neither"],
)
def test_detect_from_env_vars(env_vars: Dict[str, str], expect_interval: bool) -> None:
    with patch.dict(os.environ, env_vars, clear=False):
        os.environ.pop("DLT_INTERVAL_START", None) if "DLT_INTERVAL_START" not in env_vars else None
        os.environ.pop("DLT_INTERVAL_END", None) if "DLT_INTERVAL_END" not in env_vars else None
        ctx = TimeIntervalContext()
        # `interval` re-detects on access when not set explicitly, so the
        # assertion must happen inside the patch.dict scope
        if expect_interval:
            assert ctx.interval is not None
            assert ctx.interval[0] == ensure_datetime(env_vars["DLT_INTERVAL_START"])
            assert ctx.interval[1] == ensure_datetime(env_vars["DLT_INTERVAL_END"])
        else:
            assert ctx.interval is None


@pytest.mark.parametrize(
    "airflow_context,expect_interval",
    [
        # scheduled run: proper interval
        (
            {
                "data_interval_start": pendulum.datetime(2024, 1, 15, tz="UTC"),
                "data_interval_end": pendulum.datetime(2024, 1, 16, tz="UTC"),
            },
            True,
        ),
        # manual run: start == end, passed through as-is
        (
            {
                "data_interval_start": pendulum.datetime(2024, 6, 1, tz="UTC"),
                "data_interval_end": pendulum.datetime(2024, 6, 1, tz="UTC"),
            },
            True,
        ),
        # asset-triggered run: no intervals at all
        ({}, False),
        # asset-triggered run: keys present but None
        ({"data_interval_start": None, "data_interval_end": None}, False),
        # partial: start present, end None
        (
            {
                "data_interval_start": pendulum.datetime(2024, 1, 15, tz="UTC"),
                "data_interval_end": None,
            },
            False,
        ),
    ],
    ids=["scheduled", "manual", "asset-no-keys", "asset-none-values", "partial-start-only"],
)
def test_detect_from_airflow(
    airflow_context: Dict[str, Optional[pendulum.DateTime]],
    expect_interval: bool,
) -> None:
    """Airflow detection via mocked get_current_context."""
    mock_module = MagicMock()
    mock_module.get_current_context.return_value = airflow_context
    with (
        patch.dict(os.environ, {}, clear=False),
        patch.dict("sys.modules", {"airflow.operators.python": mock_module}),
    ):
        os.environ.pop("DLT_INTERVAL_START", None)
        os.environ.pop("DLT_INTERVAL_END", None)
        ctx = TimeIntervalContext()
        # `interval` re-detects on access when not set explicitly, so the
        # assertion must happen inside the mock-airflow patch scope
        if expect_interval:
            assert ctx.interval is not None
            assert ctx.interval[0] == airflow_context["data_interval_start"]
            assert ctx.interval[1] == airflow_context["data_interval_end"]
        else:
            assert ctx.interval is None


def test_injectable_context_and_current() -> None:
    """Injected context accessible via get_interval_context and dlt.current.interval."""
    iv = _utc_iv("2024-06-01T00:00:00Z", "2024-06-02T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        assert get_interval_context() is ctx
        current_iv = dlt.current.interval()
        assert current_iv == iv


@pytest.mark.parametrize(
    "iv_tz,expected_tz_name,expected_start,expected_end",
    [
        # tz env var set → UTC parsed then converted to target tz
        (
            "Europe/Berlin",
            "Europe/Berlin",
            datetime(2024, 1, 15, 1, tzinfo=ZoneInfo("Europe/Berlin")),  # 00:00Z = 01:00 CET
            datetime(2024, 1, 16, 1, tzinfo=ZoneInfo("Europe/Berlin")),
        ),
        (
            "America/New_York",
            "America/New_York",
            datetime(2024, 1, 14, 19, tzinfo=ZoneInfo("America/New_York")),  # 00:00Z = 19:00 EST
            datetime(2024, 1, 15, 19, tzinfo=ZoneInfo("America/New_York")),
        ),
        # no tz env var → UTC passthrough (stdlib timezone.utc)
        (
            None,
            None,
            datetime(2024, 1, 15, tzinfo=timezone.utc),
            datetime(2024, 1, 16, tzinfo=timezone.utc),
        ),
        # an explicit UTC resolves to the stdlib singleton, not to a `ZoneInfo`
        (
            "UTC",
            None,
            datetime(2024, 1, 15, tzinfo=timezone.utc),
            datetime(2024, 1, 16, tzinfo=timezone.utc),
        ),
    ],
    ids=["berlin", "new-york", "no-tz-defaults-utc", "explicit-utc"],
)
def test_detect_applies_interval_timezone_env_var(
    iv_tz: Optional[str],
    expected_tz_name: Optional[str],
    expected_start: datetime,
    expected_end: datetime,
) -> None:
    """`DLT_INTERVAL_TIMEZONE` (optional) is applied to UTC ISO env values."""
    env = {
        "DLT_INTERVAL_START": "2024-01-15T00:00:00Z",
        "DLT_INTERVAL_END": "2024-01-16T00:00:00Z",
    }
    if iv_tz is not None:
        env["DLT_INTERVAL_TIMEZONE"] = iv_tz
    with patch.dict(os.environ, env, clear=False):
        if iv_tz is None:
            os.environ.pop("DLT_INTERVAL_TIMEZONE", None)
        ctx = TimeIntervalContext()
        # `interval` re-detects on access when not set explicitly, so the
        # assertions must happen inside the patch.dict scope
        assert ctx.interval is not None
        assert ctx.interval[0] == expected_start
        assert ctx.interval[1] == expected_end
        if expected_tz_name is not None:
            assert isinstance(ctx.interval[0].tzinfo, ZoneInfo)
            assert ctx.interval[0].tzinfo.key == expected_tz_name
        else:
            assert ctx.interval[0].tzinfo == timezone.utc


def test_detect_rejects_invalid_interval_timezone() -> None:
    """The env var goes through the same validation as `TimezoneContext`."""
    env = {
        "DLT_INTERVAL_START": "2024-01-15T00:00:00Z",
        "DLT_INTERVAL_END": "2024-01-16T00:00:00Z",
        "DLT_INTERVAL_TIMEZONE": "Nowhere/Bogus",
    }
    with patch.dict(os.environ, env, clear=False):
        with pytest.raises(InvalidTimezoneName) as exc:
            TimeIntervalContext().interval
    assert exc.value.timezone == "Nowhere/Bogus"


def test_context_preserves_timezone() -> None:
    """Timezone-aware datetimes are preserved — not forced to UTC."""
    ny_tz = pendulum.timezone("America/New_York")
    start = pendulum.datetime(2024, 1, 15, 8, tz=ny_tz)
    end = pendulum.datetime(2024, 1, 16, 8, tz=ny_tz)
    ctx = TimeIntervalContext(interval=TTimeInterval(start, end))
    assert ctx.interval == (start, end)
    assert ctx.interval[0].tzinfo is not None
    assert str(ctx.interval[0].utcoffset()) == "-1 day, 19:00:00"  # UTC-5 for Jan NY


def test_incremental_with_explicit_context() -> None:
    """Incremental picks up interval from explicitly injected TimeIntervalContext."""

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {
            "updated_at": pendulum.datetime(2024, 1, 15, 12, tz="UTC"),
            "state": updated_at.get_state(),
        }

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == pendulum.datetime(2024, 1, 15, tz="UTC")
    assert inc.end_value == pendulum.datetime(2024, 1, 16, tz="UTC")


def test_join_scheduler_with_pendulum_type() -> None:
    """A pendulum-typed incremental joins the interval; the bounds arrive as stdlib datetimes."""
    initial_value = pendulum.datetime(2000, 1, 1, tz="UTC")

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
            "updated_at", initial_value=initial_value, allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": pendulum.datetime(2024, 1, 15, 12, tz="UTC")}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    with Container().injectable_context(TimeIntervalContext(interval=iv)):
        r = my_resource()
        assert len(list(r)) == 1

    inc = r.incremental._incremental
    assert inc.initial_value == pendulum.datetime(2024, 1, 15, tz="UTC")
    assert inc.end_value == pendulum.datetime(2024, 1, 16, tz="UTC")
    assert type(inc.initial_value) is type(inc.end_value) is datetime


def test_incremental_raises_when_no_interval() -> None:
    """Context with `allow_external_schedulers=True` and no interval forces a strict raise."""

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": pendulum.datetime(2024, 1, 15, 12, tz="UTC")}

    # ctx.allow_external_schedulers=True forces strict mode: missing interval must raise
    ctx = TimeIntervalContext(allow_external_schedulers=True)
    with Container().injectable_context(ctx):
        r = my_resource()
        with pytest.raises(ExternalSchedulerNotAvailable):
            list(r)


def test_decorator_incremental_with_interval_context() -> None:
    """Decorator provides incremental as fallback when param default is None."""

    @dlt.resource(incremental=dlt.sources.incremental("updated_at", allow_external_schedulers=True))
    def my_resource(
        updated_at: dlt.sources.incremental[datetime] = None,
    ):
        yield {"updated_at": pendulum.datetime(2024, 1, 15, 12, tz="UTC")}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    assert r.incremental._incremental.allow_external_schedulers is True
    assert r.incremental._incremental.end_value == pendulum.datetime(2024, 1, 16, tz="UTC")


def test_allow_external_schedulers_from_config() -> None:
    """allow_external_schedulers resolved from env var config path."""

    @dlt.resource(
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=pendulum.datetime(2024, 1, 1, tz="UTC"),
            allow_external_schedulers=True,
        )
    )
    def scheduled_resource(
        updated_at: dlt.sources.incremental[datetime] = None,
    ):
        yield {
            "updated_at": pendulum.datetime(2024, 1, 15, 12, tz="UTC"),
            "state": updated_at.get_state(),
        }

    env = {
        "UPDATED_AT__ALLOW_EXTERNAL_SCHEDULERS": "true",
    }
    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with patch.dict(os.environ, env), Container().injectable_context(ctx):
        r = scheduled_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == pendulum.datetime(2024, 1, 15, tz="UTC")
    assert inc.end_value == pendulum.datetime(2024, 1, 16, tz="UTC")


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_join_env_scheduler(item_type: TestDataItemFormat) -> None:
    d1 = pendulum.datetime(2024, 1, 1, tz="UTC")
    d2 = pendulum.datetime(2024, 1, 2, tz="UTC")
    d3 = pendulum.datetime(2024, 1, 3, tz="UTC")

    @dlt.resource
    def test_type_2(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        )
    ):
        data = [{"updated_at": d} for d in [d1, d2, d3]]
        yield data_to_item_format(item_type, data)

    # wide range [d2, d4) over [d1, d2, d3] → d2, d3
    os.environ["DLT_INTERVAL_START"] = "2024-01-02T00:00:00Z"
    os.environ["DLT_INTERVAL_END"] = "2024-01-04T00:00:00Z"
    with Container().injectable_context(TimeIntervalContext()):
        result = list(test_type_2())
    assert len(data_item_to_list(item_type, result)) == 2

    # narrower range [d2, d3) → d2
    os.environ["DLT_INTERVAL_END"] = "2024-01-03T00:00:00Z"
    with Container().injectable_context(TimeIntervalContext()):
        result = list(test_type_2())
    assert len(data_item_to_list(item_type, result)) == 1


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_join_env_scheduler_pipeline(item_type: TestDataItemFormat) -> None:
    d1 = pendulum.datetime(2024, 1, 1, tz="UTC")
    d2 = pendulum.datetime(2024, 1, 2, tz="UTC")
    d3 = pendulum.datetime(2024, 1, 3, tz="UTC")

    @dlt.resource
    def test_type_2(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        )
    ):
        data = [{"updated_at": d} for d in [d1, d2, d3]]
        yield data_to_item_format(item_type, data)

    pip_1_name = "incremental_" + uniq_id()
    pipeline = dlt.pipeline(pipeline_name=pip_1_name, destination="duckdb")

    # range [d2, d3) → d2; mock state (end_value set)
    os.environ["DLT_INTERVAL_START"] = "2024-01-02T00:00:00Z"
    os.environ["DLT_INTERVAL_END"] = "2024-01-03T00:00:00Z"
    with Container().injectable_context(TimeIntervalContext()):
        r = test_type_2()
        r.add_step(AssertItems([{"updated_at": d2}], item_type))
        pipeline.extract(r)

    # same range, fresh injection extracts same items (mock state, not persisted)
    with Container().injectable_context(TimeIntervalContext()):
        r = test_type_2()
        r.add_step(AssertItems([{"updated_at": d2}], item_type))
        pipeline.extract(r)

    # shift start earlier, widen range to [d1, d3) → d1, d2
    os.environ["DLT_INTERVAL_START"] = "2024-01-01T00:00:00Z"
    with Container().injectable_context(TimeIntervalContext()):
        r = test_type_2()
        r.add_step(AssertItems([{"updated_at": d1}, {"updated_at": d2}], item_type))
        pipeline.extract(r)


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_allow_external_schedulers(item_type: TestDataItemFormat) -> None:
    d1 = pendulum.datetime(2024, 1, 1, tz="UTC")
    d2 = pendulum.datetime(2024, 1, 2, tz="UTC")
    d3 = pendulum.datetime(2024, 1, 3, tz="UTC")

    @dlt.resource()
    def test_type_dt():
        data = [{"updated_at": d} for d in [d1, d2, d3]]
        yield data_to_item_format(item_type, data)

    # add incremental dynamically with datetime type; range [d2, d4) → d2, d3
    os.environ["DLT_INTERVAL_START"] = "2024-01-02T00:00:00Z"
    os.environ["DLT_INTERVAL_END"] = "2024-01-04T00:00:00Z"
    with Container().injectable_context(TimeIntervalContext()):
        r = test_type_dt()
        r.add_step(dlt.sources.incremental[datetime]("updated_at"))
        r.incremental.allow_external_schedulers = True
        result = data_item_to_list(item_type, list(r))
    assert len(result) == 2

    # untyped incremental raises JoinSchedulerError during type validation
    with Container().injectable_context(TimeIntervalContext()):
        r = test_type_dt()
        r.add_step(dlt.sources.incremental("updated_at"))
        r.incremental.allow_external_schedulers = True
        with pytest.raises(JoinSchedulerError):
            list(r)


def _dt(day: str) -> pendulum.DateTime:
    return pendulum.parse(day + "T00:00:00Z")  # type: ignore[return-value]


@pytest.mark.parametrize(
    "last_value_func,configured_initial,configured_end,sched_start,sched_end,"
    "data_items,expect_initial,expect_end,expect_count",
    [
        # max: start clipped — scheduler [May, Sep) → clipped to [Jun, Sep)
        (
            max,
            "2024-06-01",
            None,
            "2024-05-01",
            "2024-09-01",
            ["2024-05-15", "2024-06-15", "2024-07-15"],
            "2024-06-01",
            "2024-09-01",
            2,
        ),
        # max: end clipped — scheduler [Jun, Feb+1) → clipped to [Jun, Dec)
        (
            max,
            "2024-01-01",
            "2024-12-01",
            "2024-06-01",
            "2025-02-01",
            ["2024-07-15", "2024-11-15", "2025-01-15"],
            "2024-06-01",
            "2024-12-01",
            2,
        ),
        # max: completely outside → negative range → all filtered
        (
            max,
            "2024-06-01",
            "2024-12-01",
            "2025-01-01",
            "2025-02-01",
            ["2024-07-15", "2025-01-15"],
            "2025-01-01",
            "2024-12-01",
            0,
        ),
        # max: inside bounds → no clip
        (
            max,
            "2024-05-01",
            "2024-12-01",
            "2024-07-01",
            "2024-09-01",
            ["2024-06-15", "2024-07-15", "2024-08-15", "2024-10-15"],
            "2024-07-01",
            "2024-09-01",
            2,
        ),
        # max: no configured bounds → scheduler as-is
        (
            max,
            None,
            None,
            "2024-07-01",
            "2024-09-01",
            ["2024-06-15", "2024-07-15", "2024-08-15", "2024-10-15"],
            "2024-07-01",
            "2024-09-01",
            2,
        ),
        # min: for descending cursors, initial_value is the upper bound and
        # end_value is the lower. clipping rule mirrors max: cfg wins when
        # `last_value_func((cfg, sched)) == cfg`, which for min means cfg is
        # smaller. result: cfg always narrows the active range.
        # case A: cfg_initial < sched (narrows upper), cfg_end > sched (narrows lower)
        (
            min,
            "2024-08-01",
            "2024-06-01",
            "2024-09-01",
            "2024-05-01",
            ["2024-09-15", "2024-08-15", "2024-07-15", "2024-06-15", "2024-05-15"],
            "2024-08-01",
            "2024-06-01",
            2,
        ),
        # case B: cfg_initial > sched (no clip on initial), cfg_end > sched (clip on end)
        (
            min,
            "2024-09-01",
            "2024-07-01",
            "2024-08-01",
            "2024-05-01",
            ["2024-08-15", "2024-07-15", "2024-06-15"],
            "2024-08-01",
            "2024-07-01",
            1,
        ),
    ],
    ids=[
        "max-start-clipped",
        "max-end-clipped",
        "max-empty-negative-range",
        "max-inside-no-clip",
        "max-no-bounds",
        "min-both-clipped",
        "min-end-clipped",
    ],
)
def test_scheduler_range_clipping(
    last_value_func: Any,
    configured_initial: Optional[str],
    configured_end: Optional[str],
    sched_start: str,
    sched_end: str,
    data_items: List[str],
    expect_initial: str,
    expect_end: str,
    expect_count: int,
) -> None:
    """Scheduler range is clipped against configured initial_value/end_value."""
    cfg_initial = _dt(configured_initial) if configured_initial else None
    cfg_end = _dt(configured_end) if configured_end else None

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at",
            initial_value=cfg_initial,
            end_value=cfg_end,
            last_value_func=last_value_func,
            allow_external_schedulers=True,
        ),
    ):
        for day in data_items:
            yield {"updated_at": _dt(day)}

    iv = _utc_iv(sched_start + "T00:00:00Z", sched_end + "T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == expect_count
    inc = r.incremental._incremental
    assert inc.initial_value == _dt(expect_initial)
    assert inc.end_value == _dt(expect_end)


@pytest.mark.parametrize(
    "incr_aes,ctx_aes,expect_joined",
    [
        # user-set per-incremental wins; context only fills in when user setting is None
        (True, False, True),
        (False, True, False),
        (True, True, True),
        (False, False, False),
        # user setting is None: context fills in
        (None, True, True),
        (None, False, False),
        # both None: no join
        (None, None, False),
    ],
    ids=[
        "user-true-wins",
        "user-false-wins",
        "user-true-ctx-true",
        "user-false-ctx-false",
        "ctx-fills-in-true",
        "ctx-fills-in-false",
        "both-none",
    ],
)
def test_context_allow_external_schedulers_flag(
    incr_aes: Optional[bool], ctx_aes: Optional[bool], expect_joined: bool
) -> None:
    """User-set `allow_external_schedulers` wins over context; context `True` joins
    unset (None) incrementals, `False` does nothing. The flag is never written back."""
    initial = pendulum.datetime(2024, 1, 1, tz="UTC")

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at",
            initial_value=initial,
            allow_external_schedulers=incr_aes,
        ),
    ):
        yield {"updated_at": pendulum.datetime(2024, 7, 15, tz="UTC")}

    iv = _utc_iv("2024-07-01T00:00:00Z", "2024-08-01T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv, allow_external_schedulers=ctx_aes)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    if expect_joined:
        assert inc.initial_value == pendulum.datetime(2024, 7, 1, tz="UTC")
        assert inc.end_value == pendulum.datetime(2024, 8, 1, tz="UTC")
    else:
        assert inc.initial_value == initial
        assert inc.end_value is None
    # the flag is never written back: it stays exactly as the user set it (or None)
    assert inc.allow_external_schedulers is incr_aes


def test_str_cursor_raises_join_error() -> None:
    """str cursor type is rejected when joining external scheduler."""

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": "2024-01-15"}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        with pytest.raises(JoinSchedulerError, match="str"):
            list(r)


def test_any_cursor_raises_join_error() -> None:
    """Untyped incremental is rejected when joining external scheduler."""

    @dlt.resource()
    def my_resource(
        updated_at=dlt.sources.incremental("updated_at", allow_external_schedulers=True),
    ):
        yield {"updated_at": "2024-01-15"}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        with pytest.raises(JoinSchedulerError, match="data type"):
            list(r)


def test_date_cursor_with_datetime_interval() -> None:
    """date cursor works — datetime interval is coerced to date."""

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[date] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": date(2024, 1, 15)}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == date(2024, 1, 15)
    assert inc.end_value == date(2024, 1, 16)


def test_date_cursor_non_midnight_interval() -> None:
    """date cursor with non-midnight scheduler times — time is truncated."""

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[date] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": date(2024, 1, 15)}

    iv = _utc_iv("2024-01-15T06:30:00Z", "2024-01-16T18:45:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == date(2024, 1, 15)
    assert inc.end_value == date(2024, 1, 16)


def test_float_cursor_as_timestamp() -> None:
    """float cursor gets unix timestamp from datetime interval."""
    start_ts = pendulum.datetime(2024, 1, 15, tz="UTC").timestamp()
    end_ts = pendulum.datetime(2024, 1, 16, tz="UTC").timestamp()
    mid_ts = pendulum.datetime(2024, 1, 15, 12, tz="UTC").timestamp()

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[float] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": mid_ts}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == start_ts
    assert inc.end_value == end_ts


def test_naive_datetime_cursor_with_tz_aware_scheduler() -> None:
    """Naive datetime initial_value is adapted to match tz-aware scheduler values."""

    # case 1: no configured bounds → works (no clipping needed)
    @dlt.resource()
    def no_bounds(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": datetime(2024, 1, 15, 12)}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = no_bounds()
        items = list(r)
    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == pendulum.datetime(2024, 1, 15, tz="UTC")

    # case 2: naive configured bounds → scheduler values adapted to naive
    @dlt.resource()
    def with_naive_bounds(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at",
            initial_value=datetime(2024, 1, 1),  # noqa: B008
            allow_external_schedulers=True,
        ),
    ):
        yield {"updated_at": datetime(2024, 1, 15, 12)}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = with_naive_bounds()
        items = list(r)

    # scheduler initial adapted to naive (matching configured), end stays tz-aware (no configured end)
    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == datetime(2024, 1, 15)
    assert inc.initial_value.tzinfo is None
    # end_value stays tz-aware — no configured_end to adapt to
    assert inc.end_value == pendulum.datetime(2024, 1, 16, tz="UTC")

    # case 3: naive configured bounds that CLIP the scheduler range
    @dlt.resource()
    def with_clipping_naive_bounds(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
            "updated_at",
            initial_value=datetime(2024, 1, 15, 6),  # noqa: B008
            allow_external_schedulers=True,
        ),
    ):
        yield {"updated_at": datetime(2024, 1, 15, 12)}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = with_clipping_naive_bounds()
        items = list(r)

    # configured initial (06:00) clips scheduler start (00:00), stays naive
    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == datetime(2024, 1, 15, 6)
    assert inc.initial_value.tzinfo is None


def test_int_cursor_as_timestamp() -> None:
    """int cursor gets integer unix timestamp from datetime interval."""
    start_ts = int(pendulum.datetime(2024, 1, 15, tz="UTC").timestamp())
    end_ts = int(pendulum.datetime(2024, 1, 16, tz="UTC").timestamp())
    mid_ts = int(pendulum.datetime(2024, 1, 15, 12, tz="UTC").timestamp())

    @dlt.resource()
    def my_resource(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": mid_ts}

    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    with Container().injectable_context(ctx):
        r = my_resource()
        items = list(r)

    assert len(items) == 1
    inc = r.incremental._incremental
    assert inc.initial_value == start_ts
    assert inc.end_value == end_ts


@pytest.mark.parametrize("local_tz", LOCAL_TIMEZONES)
def test_numeric_cursor_from_naive_interval(local_tz: str) -> None:
    """A naive interval is read as UTC, so the bounds do not depend on the machine timezone."""
    # 2024-01-15T00:00:00Z and the following midnight
    start_ts, end_ts = 1705276800, 1705363200

    @dlt.resource()
    def int_cursor(
        updated_at: dlt.sources.incremental[int] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": 1705320000}

    @dlt.resource()
    def float_cursor(
        updated_at: dlt.sources.incremental[float] = dlt.sources.incremental(
            "updated_at", allow_external_schedulers=True
        ),
    ):
        yield {"updated_at": 1705320000.5}

    iv = TTimeInterval(
        ensure_datetime("2024-01-15T00:00:00"), ensure_datetime("2024-01-16T00:00:00")
    )
    assert iv.start.tzinfo is None
    with local_timezone(local_tz):
        with Container().injectable_context(TimeIntervalContext(interval=iv)):
            for resource, expected_type in ((int_cursor(), int), (float_cursor(), float)):
                assert len(list(resource)) == 1
                inc = resource.incremental._incremental
                assert inc.initial_value == start_ts
                assert inc.end_value == end_ts
                assert type(inc.initial_value) is expected_type


def test_accessor_get_and_set() -> None:
    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    new_iv = _utc_iv("2023-06-01T00:00:00Z", "2023-12-31T00:00:00Z")
    with Container().injectable_context(TimeIntervalContext()):
        # empty context reads as None
        assert _interval_accessor() is None
        # set populates
        _interval_accessor.set(iv)
        assert _interval_accessor() == iv
        # set replaces
        _interval_accessor.set(new_iv)
        assert _interval_accessor() == new_iv
        # set(None) clears
        _interval_accessor.set(None)
        assert _interval_accessor() is None


def test_accessor_is_empty() -> None:
    """`is_empty` flags missing and zero-length intervals (manual and event runs)."""
    # no context at all
    assert _interval_accessor.is_empty
    with Container().injectable_context(TimeIntervalContext()):
        # context without an interval
        assert _interval_accessor.is_empty
        # zero-length [now, now) interval of a manual dispatch
        now = pendulum.now("UTC")
        _interval_accessor.set(TTimeInterval(now, now))
        assert _interval_accessor.is_empty
        # real interval
        _interval_accessor.set(_utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z"))
        assert not _interval_accessor.is_empty


@pytest.mark.parametrize(
    "kwargs,expected_start,expected_end",
    [
        (
            {"start": "2023-01-01T00:00:00Z"},
            "2023-01-01T00:00:00Z",
            "2024-01-16T00:00:00Z",
        ),
        (
            {"end": "2024-02-01T00:00:00Z"},
            "2024-01-15T00:00:00Z",
            "2024-02-01T00:00:00Z",
        ),
        (
            {"start": "2023-01-01T00:00:00Z", "end": "2025-01-01T00:00:00Z"},
            "2023-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z",
        ),
    ],
    ids=["start-only", "end-only", "both"],
)
def test_accessor_update(kwargs: Dict[str, str], expected_start: str, expected_end: str) -> None:
    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    parsed = {k: ensure_pendulum_datetime(v) for k, v in kwargs.items()}
    with Container().injectable_context(TimeIntervalContext(interval=iv)):
        _interval_accessor.update(**parsed)
        assert _interval_accessor() == _utc_iv(expected_start, expected_end)


def test_accessor_update_raises_when_no_interval() -> None:
    with (
        patch.dict(os.environ, {}, clear=True),
        Container().injectable_context(TimeIntervalContext()),
    ):
        with pytest.raises(RuntimeError, match="no active interval to update"):
            _interval_accessor.update(start=ensure_pendulum_datetime("2024-01-01T00:00:00Z"))


def test_accessor_apply_lag_and_full_days() -> None:
    """`dlt.current.interval` mutators chain and replicate the manual lag-and-widen pattern."""
    iv = _utc_iv("2024-01-13T07:00:00Z", "2024-01-15T14:00:00Z")
    with Container().injectable_context(TimeIntervalContext(interval=iv)):
        assert not _interval_accessor.is_empty
        # mutators return the accessor so calls chain; result reads via the call form
        assert _interval_accessor.apply_full_days().apply_lag("0 0 * * *", 3)() == _utc_iv(
            "2024-01-10T00:00:00Z", "2024-01-16T00:00:00Z"
        )
        # mutators set the active interval
        assert _interval_accessor() == _utc_iv("2024-01-10T00:00:00Z", "2024-01-16T00:00:00Z")

    # no active interval raises
    with Container().injectable_context(TimeIntervalContext()):
        with pytest.raises(RuntimeError, match="no active interval"):
            _interval_accessor.apply_lag("0 0 * * *")
        with pytest.raises(RuntimeError, match="no active interval"):
            _interval_accessor.apply_full_days()


def test_accessor_timezone() -> None:
    berlin = ZoneInfo("Europe/Berlin")
    # without an interval to carry a zone, the context timezone answers
    assert _interval_accessor.timezone == timezone.utc
    with Container().injectable_context(TimeIntervalContext()):
        assert _interval_accessor.timezone == timezone.utc
        _interval_accessor.set(
            TTimeInterval(
                datetime(2024, 1, 15, tzinfo=berlin), datetime(2024, 1, 16, tzinfo=berlin)
            )
        )
        # the named zone survives, so it can be passed back to ensure_datetime_in_tz
        assert _interval_accessor.timezone == berlin
        _interval_accessor.set(
            TTimeInterval(
                datetime(2024, 1, 15, tzinfo=timezone.utc),
                datetime(2024, 1, 16, tzinfo=timezone.utc),
            )
        )
        assert _interval_accessor.timezone == timezone.utc
        # a naive interval carries no zone, so the context timezone answers instead
        _interval_accessor.set(TTimeInterval(datetime(2024, 1, 15), datetime(2024, 1, 16)))
        assert _interval_accessor.timezone == timezone.utc
        # taken from the start, so a mixed interval reports the start's zone
        _interval_accessor.set(
            TTimeInterval(datetime(2024, 1, 15, tzinfo=berlin), datetime(2024, 1, 16))
        )
        assert _interval_accessor.timezone == berlin


@pytest.mark.parametrize(
    "new_start,expected_wall_clock",
    [
        # naive datetime is wall clock in the interval's zone
        (datetime(2024, 1, 10, 6, 0), datetime(2024, 1, 10, 6, 0)),
        # a bare date string is midnight in the interval's zone
        ("2024-01-10", datetime(2024, 1, 10, 0, 0)),
        ("2024-01-10T06:00:00", datetime(2024, 1, 10, 6, 0)),
        # an aware value is converted into the interval's zone, keeping the instant.
        # 05:00 UTC is 06:00 in Berlin in January (UTC+1)
        (datetime(2024, 1, 10, 5, 0, tzinfo=timezone.utc), datetime(2024, 1, 10, 6, 0)),
        ("2024-01-10T05:00:00+00:00", datetime(2024, 1, 10, 6, 0)),
    ],
    ids=["naive-datetime", "date-string", "naive-string", "aware-datetime", "offset-string"],
)
def test_accessor_update_keeps_interval_timezone(
    new_start: Any, expected_wall_clock: datetime
) -> None:
    """`update` takes new bounds into the interval's timezone, so both ends keep one zone."""
    berlin = ZoneInfo("Europe/Berlin")
    iv = TTimeInterval(datetime(2024, 1, 15, tzinfo=berlin), datetime(2024, 1, 16, tzinfo=berlin))
    with Container().injectable_context(TimeIntervalContext(interval=iv)):
        _interval_accessor.update(start=new_start)
        updated = _interval_accessor()
        assert updated.start == expected_wall_clock.replace(tzinfo=berlin)
        # the untouched end and the whole interval stay in the job's zone
        assert updated.end == iv.end
        assert updated.start.tzinfo == updated.end.tzinfo == berlin
        assert _interval_accessor.timezone == berlin


def test_naive_interval_is_read_in_context_timezone() -> None:
    """Bounds are always tz-aware: a naive one is read in the context timezone, never the local."""
    iv = TTimeInterval(datetime(2024, 1, 15), datetime(2024, 1, 16))
    with Container().injectable_context(TimeIntervalContext(interval=iv)):
        assert _interval_accessor() == TTimeInterval(
            datetime(2024, 1, 15, tzinfo=timezone.utc),
            datetime(2024, 1, 16, tzinfo=timezone.utc),
        )
        assert _interval_accessor.timezone == timezone.utc
        # an update stays in that zone
        _interval_accessor.update(start="2024-01-10T06:00:00")
        assert _interval_accessor()[0] == datetime(2024, 1, 10, 6, tzinfo=timezone.utc)


def test_interval_does_not_install_timezone() -> None:
    """The zone the bounds carry stays on the interval; the context timezone is set by the run."""
    berlin = ZoneInfo("Europe/Berlin")
    iv = TTimeInterval(datetime(2024, 1, 15, tzinfo=berlin), datetime(2024, 1, 16, tzinfo=berlin))
    with Container().injectable_context(TimeIntervalContext(interval=iv)):
        assert dlt.current.interval.timezone == berlin
        assert dlt.current.timezone() == timezone.utc
        assert TimezoneContext not in Container()
