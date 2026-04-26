"""Tests for TimeIntervalContext — creation, detection, and dlt.current.interval()."""

import os
import time
from datetime import date, datetime, timezone  # noqa: I251
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

import dlt
from dlt.common.configuration.container import Container
from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_non_utc, ensure_pendulum_datetime_utc
from dlt.common.typing import TTimeInterval
from dlt.common.utils import uniq_id
from dlt.extract.incremental.context import TimeIntervalContext, get_interval_context
from dlt.extract.incremental.exceptions import ExternalSchedulerNotAvailable, JoinSchedulerError

from tests.extract.utils import AssertItems, data_item_to_list
from tests.utils import (
    ALL_TEST_DATA_ITEM_FORMATS,
    TestDataItemFormat,
    data_to_item_format,
)


def _utc_iv(start: str, end: str) -> TTimeInterval:
    """Build a UTC interval from ISO strings."""
    return (ensure_pendulum_datetime_utc(start), ensure_pendulum_datetime_utc(end))


def test_explicit_context_with_tuple() -> None:
    """Created with (start, end) tuple, .interval returns it."""
    iv = _utc_iv("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z")
    ctx = TimeIntervalContext(interval=iv)
    assert ctx.interval == iv


def test_explicit_context_with_pendulum() -> None:
    start = pendulum.datetime(2024, 6, 1, tz="UTC")
    end = pendulum.datetime(2024, 6, 2, tz="UTC")
    ctx = TimeIntervalContext(interval=(start, end))
    assert ctx.interval == (start, end)


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
    if expect_interval:
        assert ctx.interval is not None
        assert ctx.interval[0] == ensure_pendulum_datetime_non_utc(env_vars["DLT_INTERVAL_START"])
        assert ctx.interval[1] == ensure_pendulum_datetime_non_utc(env_vars["DLT_INTERVAL_END"])
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
    ],
    ids=["berlin", "new-york", "no-tz-defaults-utc"],
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
    assert ctx.interval is not None
    assert ctx.interval[0] == expected_start
    assert ctx.interval[1] == expected_end
    if expected_tz_name is not None:
        assert isinstance(ctx.interval[0].tzinfo, ZoneInfo)
        assert ctx.interval[0].tzinfo.key == expected_tz_name
    else:
        assert ctx.interval[0].tzinfo == timezone.utc


def test_context_preserves_timezone() -> None:
    """Timezone-aware datetimes are preserved — not forced to UTC."""
    ny_tz = pendulum.timezone("America/New_York")
    start = pendulum.datetime(2024, 1, 15, 8, tz=ny_tz)
    end = pendulum.datetime(2024, 1, 16, 8, tz=ny_tz)
    ctx = TimeIntervalContext(interval=(start, end))
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
    "configured_initial,configured_end,sched_start,sched_end,"
    "data_items,expect_initial,expect_end,expect_count",
    [
        # start clipped: scheduler [May, Sep) → clipped to [Jun, Sep)
        (
            "2024-06-01",
            None,
            "2024-05-01",
            "2024-09-01",
            ["2024-05-15", "2024-06-15", "2024-07-15"],
            "2024-06-01",
            "2024-09-01",
            2,
        ),
        # end clipped: scheduler [Jun, Feb+1) → clipped to [Jun, Dec)
        (
            "2024-01-01",
            "2024-12-01",
            "2024-06-01",
            "2025-02-01",
            ["2024-07-15", "2024-11-15", "2025-01-15"],
            "2024-06-01",
            "2024-12-01",
            2,
        ),
        # completely outside → negative range → all filtered
        (
            "2024-06-01",
            "2024-12-01",
            "2025-01-01",
            "2025-02-01",
            ["2024-07-15", "2025-01-15"],
            "2025-01-01",
            "2024-12-01",
            0,
        ),
        # inside bounds → no clip
        (
            "2024-05-01",
            "2024-12-01",
            "2024-07-01",
            "2024-09-01",
            ["2024-06-15", "2024-07-15", "2024-08-15", "2024-10-15"],
            "2024-07-01",
            "2024-09-01",
            2,
        ),
        # no configured bounds → scheduler as-is
        (
            None,
            None,
            "2024-07-01",
            "2024-09-01",
            ["2024-06-15", "2024-07-15", "2024-08-15", "2024-10-15"],
            "2024-07-01",
            "2024-09-01",
            2,
        ),
    ],
    ids=[
        "start-clipped",
        "end-clipped",
        "empty-negative-range",
        "inside-no-clip",
        "no-bounds",
    ],
)
def test_scheduler_range_clipping(
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
        # context True forces join even though incremental is False
        (False, True, True),
        # context False prevents join even though incremental is True
        (True, False, False),
        # context None defers to incremental's own False
        (False, None, False),
        # context None defers to incremental's own True
        (True, None, True),
    ],
    ids=["ctx-forces-join", "ctx-prevents-join", "ctx-defers-false", "ctx-defers-true"],
)
def test_context_allow_external_schedulers_flag(
    incr_aes: bool, ctx_aes: bool, expect_joined: bool
) -> None:
    """allow_external_schedulers on context overrides per-incremental setting."""
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
