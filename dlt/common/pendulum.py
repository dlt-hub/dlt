from datetime import datetime, timezone, timedelta, tzinfo  # noqa: I251
from typing import Optional, SupportsIndex, Union
import pendulum  # noqa: I251
from pendulum.tz import UTC
from pendulum.tz.timezone import FixedTimezone, Timezone

# force UTC as the local timezone to prevent local dates to be written to dbs
pendulum.set_local_timezone(pendulum.timezone("UTC"))


def __utcnow() -> pendulum.DateTime:
    """
    Use this function instead of datetime.now
    Returns:
        pendulum.DateTime -- current time in UTC timezone
    """
    return pendulum.now()


def to_pendulum_tz(
    tz: Optional[tzinfo], is_utc: bool = False
) -> Optional[Union[Timezone, FixedTimezone]]:
    """Convert a tzinfo to a pendulum Timezone/FixedTimezone"""
    if tz is None:
        return None
    if is_utc:
        return UTC
    # already a pendulum timezone
    if isinstance(tz, (Timezone, FixedTimezone)):
        return tz
    # Python stdlib fixed offset - use cached fixed_timezone
    if isinstance(tz, timezone):
        offset_seconds = int(tz.utcoffset(None).total_seconds())
        if offset_seconds == 0:
            return UTC
        return pendulum.fixed_timezone(offset_seconds)
    # named timezone (pytz, dateutil, zoneinfo) - need _safe_timezone for DST
    return pendulum._safe_timezone(tz)


def create_dt(
    year: SupportsIndex,
    month: SupportsIndex,
    day: SupportsIndex,
    hour: SupportsIndex = 0,
    minute: SupportsIndex = 0,
    second: SupportsIndex = 0,
    microsecond: SupportsIndex = 0,
    tz: tzinfo = None,
    fold: int = 1,
) -> pendulum.DateTime:
    """Creates a new DateTime instance from a specific date and time."""
    pend_tz = to_pendulum_tz(tz)

    if pend_tz is None:
        # naive datetime
        return pendulum.DateTime(year, month, day, hour, minute, second, microsecond)

    if isinstance(pend_tz, FixedTimezone) or pend_tz is UTC:
        # fixed offset or UTC - no DST, no conversion needed
        return pendulum.DateTime(
            year, month, day, hour, minute, second, microsecond, tzinfo=pend_tz
        )

    # named timezone - need convert() for DST handling
    dt = datetime(year, month, day, hour, minute, second, microsecond, fold=fold)
    dt = pend_tz.convert(dt)
    return pendulum.DateTime(
        dt.year,
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        dt.microsecond,
        tzinfo=dt.tzinfo,
        fold=dt.fold,
    )


pendulum.utcnow = __utcnow  # type: ignore
