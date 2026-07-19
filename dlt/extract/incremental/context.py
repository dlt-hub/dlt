"""Time interval context for external schedulers."""

import os
from typing import ClassVar, Optional, Tuple, Union
from datetime import datetime  # noqa: I251
from zoneinfo import ZoneInfo

from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)
from dlt.common.configuration.container import Container
from dlt.common.interval import full_days_interval, lag_interval
from dlt.common.time import ensure_datetime_utc
from dlt.common.typing import TTimeInterval

TAnyTimeInterval = Union[TTimeInterval, Tuple[datetime, datetime]]
"""A `(start, end)` interval as either a `TTimeInterval` or a plain datetime tuple."""


def _to_time_interval(interval: Optional[TAnyTimeInterval]) -> Optional[TTimeInterval]:
    if interval is None or isinstance(interval, TTimeInterval):
        return interval
    return TTimeInterval(*interval)


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

    def _detect(self) -> Optional[TTimeInterval]:
        """Detect interval from environment. Order: dlt env vars -> Airflow -> None.

        `DLT_INTERVAL_START` / `DLT_INTERVAL_END` are UTC ISO 8601. An optional
        `DLT_INTERVAL_TIMEZONE` (IANA name) is applied after UTC parsing so the
        resulting interval. Partial detection (start without end, or vice versa)
        returns `None`.
        """
        start_value = os.environ.get("DLT_INTERVAL_START")
        end_value = os.environ.get("DLT_INTERVAL_END")
        if start_value and end_value:
            start_utc = ensure_datetime_utc(start_value)
            end_utc = ensure_datetime_utc(end_value)
            tz_name = os.environ.get("DLT_INTERVAL_TIMEZONE")
            if tz_name:
                tz = ZoneInfo(tz_name)
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
            raise RuntimeError("no TimeIntervalContext active")
        ctx.interval = interval

    def update(
        self,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> None:
        """Override `start` and/or `end`, preserving the other bound."""
        ctx = get_interval_context()
        cur = ctx.interval if ctx else None
        if cur is None:
            raise RuntimeError("no active interval to update")
        ctx.interval = TTimeInterval(
            start if start is not None else cur.start,
            end if end is not None else cur.end,
        )

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
            RuntimeError: If no interval is active.
        """
        ctx = get_interval_context()
        cur = ctx.interval if ctx else None
        if cur is None:
            raise RuntimeError("no active interval to lag")
        ctx.interval = lag_interval(cur, trigger, count, lag_end)
        return self

    def apply_full_days(self) -> "_IntervalAccessor":
        """Widens the active interval to full days: start floored to midnight, end
        extended to the next midnight, each in its own timezone.

        Returns:
            The accessor itself, so calls can be chained.

        Raises:
            RuntimeError: If no interval is active.
        """
        ctx = get_interval_context()
        cur = ctx.interval if ctx else None
        if cur is None:
            raise RuntimeError("no active interval to adjust")
        ctx.interval = full_days_interval(cur)
        return self

    def __str__(self) -> str:
        return str(self())


interval = _IntervalAccessor()
