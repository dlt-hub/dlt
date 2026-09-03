"""Time interval context for external schedulers."""

import os
from typing import ClassVar, Optional, Tuple, Union
from datetime import datetime, timezone as dt_timezone, tzinfo

from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)
from dlt.common import known_env
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.timezone_context import to_tzinfo
from dlt.common.interval import full_days_interval, lag_interval
from dlt.common.time import ensure_datetime, ensure_datetime_in_tz, get_context_timezone
from dlt.common.typing import TAnyDateTime, TTimeInterval
from dlt.common.utils import str2bool
from dlt.extract.incremental.exceptions import IntervalNotAvailable

TAnyTimeInterval = Union[TTimeInterval, Tuple[datetime, datetime]]
"""A `(start, end)` interval as either a `TTimeInterval` or a plain datetime tuple."""


def _bound_in_context_tz(value: TAnyDateTime) -> datetime:
    """Interval bounds are always tz-aware, so a naive one is read in the context timezone."""
    dt = ensure_datetime(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=get_context_timezone())


def _detect_allow_external_schedulers() -> Optional[bool]:
    """Process-wide flag, set by a launcher before it exec's the user module."""
    value = os.environ.get(known_env.DLT_ALLOW_EXTERNAL_SCHEDULERS)
    return str2bool(value) if value else None


def _to_time_interval(interval: Optional[TAnyTimeInterval]) -> Optional[TTimeInterval]:
    if interval is None:
        return None
    start, end = interval
    return TTimeInterval(_bound_in_context_tz(start), _bound_in_context_tz(end))


@configspec
class TimeIntervalContext(ContainerInjectableContext):
    """Active time interval from an external scheduler."""

    can_create_default: ClassVar[bool] = True
    global_affinity: ClassVar[bool] = False

    allow_external_schedulers: Optional[bool] = None
    """When `True`, enables `allow_external_schedulers` on incrementals that left it unset."""

    def __init__(
        self,
        interval: Optional[TAnyTimeInterval] = None,
        allow_external_schedulers: Optional[bool] = None,
    ) -> None:
        super().__init__()
        if allow_external_schedulers is None:
            allow_external_schedulers = _detect_allow_external_schedulers()
        self.allow_external_schedulers = allow_external_schedulers
        # explicit interval is stored; when None, `interval` property auto-detects
        # fresh on every access (so long-lived processes like an Airflow worker
        # running multiple tasks see the current `data_interval_start/end`)
        self._interval = _to_time_interval(interval)

    @property
    def interval(self) -> Optional[TTimeInterval]:
        """Resolved interval as `(start, end)` datetime tuple, or `None`."""

        if self._interval is not None:
            return self._interval
        # always autodetec if no explicit interval for lazily injected intervals
        return self._detect()

    @interval.setter
    def interval(self, interval: Optional[TAnyTimeInterval]) -> None:
        self._interval = _to_time_interval(interval)

    @property
    def timezone(self) -> tzinfo:
        """Timezone both bounds are in, which is the context timezone when there is no interval."""
        interval = self.interval
        return interval.start.tzinfo if interval else get_context_timezone()

    def _detect(self) -> Optional[TTimeInterval]:
        """Detect interval from environment. Order: dlt env vars -> Airflow -> None.

        `DLT_INTERVAL_START` / `DLT_INTERVAL_END` are UTC ISO 8601. An optional
        `DLT_INTERVAL_TIMEZONE` (IANA name) converts both bounds into that zone. Partial
        detection (start without end, or vice versa) returns `None`.
        """
        start_value = os.environ.get(known_env.DLT_INTERVAL_START)
        end_value = os.environ.get(known_env.DLT_INTERVAL_END)
        if start_value and end_value:
            start_utc = ensure_datetime_in_tz(start_value, dt_timezone.utc)
            end_utc = ensure_datetime_in_tz(end_value, dt_timezone.utc)
            tz_name = os.environ.get(known_env.DLT_INTERVAL_TIMEZONE)
            if tz_name:
                tz = to_tzinfo(tz_name)
                return TTimeInterval(start_utc.astimezone(tz), end_utc.astimezone(tz))
            return TTimeInterval(start_utc, end_utc)

        try:
            try:
                from airflow.operators.python import get_current_context  # noqa
            except ImportError:
                from airflow.sdk import get_current_context  # type: ignore[no-redef,unused-ignore]

            context = get_current_context()
            start_date = context.get("data_interval_start")
            end_date: datetime = context.get("data_interval_end")
            if start_date is not None and end_date is not None:
                return TTimeInterval(start_date, end_date)
        except Exception:
            pass

        return None


def get_interval_context() -> Optional[TimeIntervalContext]:
    """Get the active interval context from Container, or `None`."""
    return Container().get(TimeIntervalContext)


class _IntervalAccessor:
    """Callable accessor for the active interval. Exposed as `dlt.current.interval`."""

    def __call__(self) -> Optional[TTimeInterval]:
        ctx = get_interval_context()
        return ctx.interval if ctx else None

    def set(self, interval: Optional[TAnyTimeInterval]) -> None:  # noqa: A003
        ctx = get_interval_context()
        if ctx is None:
            raise IntervalNotAvailable("set", context_missing=True)
        ctx.interval = interval

    def update(
        self,
        *,
        start: Optional[TAnyDateTime] = None,
        end: Optional[TAnyDateTime] = None,
    ) -> None:
        """Override `start` and/or `end`, preserving the other bound.

        Each new bound is taken into the interval's timezone: naive values (including
        plain dates and ISO strings without an offset) are read as wall clock there,
        aware values are converted. Both ends of the interval keep a single timezone.

        Args:
            start (Optional[TAnyDateTime]): New start of the interval.
            end (Optional[TAnyDateTime]): New end of the interval.

        Raises:
            IntervalNotAvailable: If no interval is active.
        """
        ctx = get_interval_context()
        cur = ctx.interval if ctx else None
        if cur is None:
            raise IntervalNotAvailable("update")
        tz = ctx.timezone
        ctx.interval = TTimeInterval(
            cur.start if start is None else ensure_datetime_in_tz(start, tz),
            cur.end if end is None else ensure_datetime_in_tz(end, tz),
        )

    @property
    def timezone(self) -> Optional[tzinfo]:
        """Timezone of the active interval, or `None` when no interval is active."""
        ctx = get_interval_context()
        return ctx.timezone if ctx else None

    @property
    def is_empty(self) -> bool:
        """True when no interval is active or it has zero length (manual and event runs)."""
        iv = self()
        return iv is None or iv.start >= iv.end

    def apply_lag(self, trigger: str, count: int = 1, lag_end: bool = False) -> "_IntervalAccessor":
        """Lags the active interval start (or end) by `count` trigger ticks into the past.

        The bound snaps to the trigger tick grid: `count=0` floors it to the
        latest tick, negative `count` moves it into the future.

        Args:
            trigger (str): A `schedule:` or `every:` trigger, or a bare cron expression.
            count (int): Number of ticks (or `every:` periods) to lag, negative
                moves into the future. Defaults to 1.
            lag_end (bool): When `True`, adjusts the end instead of the start.

        Returns:
            The accessor itself, so calls can be chained.

        Raises:
            ValueError: If the adjusted interval is empty or negative.
            IntervalNotAvailable: If no interval is active.
        """
        ctx = get_interval_context()
        cur = ctx.interval if ctx else None
        if cur is None:
            raise IntervalNotAvailable("lag")
        ctx.interval = lag_interval(cur, trigger, count, lag_end)
        return self

    def apply_full_days(self) -> "_IntervalAccessor":
        """Widens the active interval to full days: start floored to midnight, end
        extended to the next midnight, each in its own timezone.

        Returns:
            The accessor itself, so calls can be chained.

        Raises:
            IntervalNotAvailable: If no interval is active.
        """
        ctx = get_interval_context()
        cur = ctx.interval if ctx else None
        if cur is None:
            raise IntervalNotAvailable("widen")
        ctx.interval = full_days_interval(cur)
        return self

    def __str__(self) -> str:
        return str(self())


interval = _IntervalAccessor()


def timezone() -> tzinfo:
    """The context timezone. UTC unless the run declares one through `TimezoneContext` or
    `DLT_INTERVAL_TIMEZONE`.

    Unlike `dlt.current.interval.timezone`, which describes the zone the interval bounds
    happen to carry, this is the zone `dlt` actually writes values in and is never `None`.
    """
    return get_context_timezone()
