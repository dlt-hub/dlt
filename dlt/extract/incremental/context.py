"""Time interval context for external schedulers."""

import os
from typing import Any, ClassVar, Optional, Tuple
from datetime import datetime  # noqa: I251
from zoneinfo import ZoneInfo

from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)
from dlt.common.configuration.container import Container
from dlt.common.time import ensure_datetime_utc
from dlt.common.typing import TTimeInterval


@configspec
class TimeIntervalContext(ContainerInjectableContext):
    """Active time interval from an external scheduler or dlt runtime.

    Created with a concrete `(start, end)` datetime tuple, or autodetects
    from dlt env vars / Airflow. Partial intervals (start without end) are
    treated as no interval.
    """

    can_create_default: ClassVar[bool] = True
    global_affinity: ClassVar[bool] = False

    allow_external_schedulers: Optional[bool] = None
    """Override per-incremental `allow_external_schedulers`."""

    def __init__(
        self,
        interval: Optional[TTimeInterval] = None,
        allow_external_schedulers: Optional[bool] = None,
    ) -> None:
        super().__init__()
        self.allow_external_schedulers = allow_external_schedulers
        # explicit interval is stored; when None, `interval` property auto-detects
        # fresh on every access (so long-lived processes like an Airflow worker
        # running multiple tasks see the current `data_interval_start/end`)
        self._interval = interval

    @property
    def interval(self) -> Optional[TTimeInterval]:
        """Resolved interval as `(start, end)` datetime tuple, or `None`.

        Returns the interval explicitly passed to the constructor when set;
        otherwise re-runs auto-detection on each access so callers always see
        the current runtime interval rather than a value cached at init time.
        """
        if self._interval is not None:
            return self._interval
        return self._detect()

    def _detect(self) -> Optional[TTimeInterval]:
        """Detect interval from environment. Order: dlt env vars -> Airflow -> None.

        `DLT_INTERVAL_START` / `DLT_INTERVAL_END` are UTC ISO 8601. An optional
        `DLT_INTERVAL_TIMEZONE` (IANA name) is applied after UTC parsing so the
        resulting datetimes carry the job's original timezone identity across
        JSON round-trip. Partial detection (start without end, or vice versa)
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
                return (start_utc.astimezone(tz), end_utc.astimezone(tz))
            return (start_utc, end_utc)

        try:
            try:
                from airflow.operators.python import get_current_context  # noqa
            except ImportError:
                from airflow.sdk import get_current_context  # type: ignore[no-redef,unused-ignore]

            context = get_current_context()
            start_date = context.get("data_interval_start")
            end_date: datetime = context.get("data_interval_end")
            if start_date is not None and end_date is not None:
                return (start_date, end_date)
        except Exception:
            pass

        return None


def get_interval_context() -> Optional[TimeIntervalContext]:
    """Get the active interval context from Container, or `None`."""
    return Container().get(TimeIntervalContext)
