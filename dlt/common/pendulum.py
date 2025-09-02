from datetime import datetime, timezone, timedelta, tzinfo  # noqa: I251
from typing import SupportsIndex
import pendulum  # noqa: I251


# force UTC as the local timezone to prevent local dates to be written to dbs
pendulum.set_local_timezone(pendulum.timezone("UTC"))


def __utcnow() -> pendulum.DateTime:
    """
    Use this function instead of datetime.now
    Returns:
        pendulum.DateTime -- current time in UTC timezone
    """
    return pendulum.now()


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
    """
    Creates a new DateTime instance from a specific date and time.
    """
    if tz is not None:
        tz = pendulum._safe_timezone(tz)

    dt = datetime(year, month, day, hour, minute, second, microsecond, fold=fold)

    if tz is not None:
        dt = tz.convert(dt)

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
