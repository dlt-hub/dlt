"""Time interval context for external schedulers."""

import os
from typing import Any, ClassVar, Optional, Tuple
from datetime import datetime  # noqa: I251

from dlt.common.configuration.specs.base_configuration import (
    ContainerInjectableContext,
    configspec,
)
from dlt.common.configuration.container import Container
from dlt.common.time import ensure_pendulum_datetime_non_utc
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

    interval: Optional[TTimeInterval] = None
    """Resolved interval as `(start, end)` datetime tuple, or `None`."""
    allow_external_schedulers: Optional[bool] = None
    """Override per-incremental `allow_external_schedulers`."""

    def __init__(
        self,
        interval: Optional[TTimeInterval] = None,
        allow_external_schedulers: Optional[bool] = None,
    ) -> None:
        super().__init__()
        self.allow_external_schedulers = allow_external_schedulers
        if interval is not None:
            self.interval = interval
        else:
            self.interval = self._detect()

    def _detect(self) -> Optional[TTimeInterval]:
        """Detect interval from environment. Order: dlt env vars -> Airflow -> None.

        Partial detection (start without end, or vice versa) returns None.
        """
        start_value = os.environ.get("DLT_START_VALUE")
        end_value = os.environ.get("DLT_END_VALUE")
        if start_value and end_value:
            return (
                ensure_pendulum_datetime_non_utc(start_value),
                ensure_pendulum_datetime_non_utc(end_value),
            )

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
