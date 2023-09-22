import contextlib
from typing import Any, Optional, Union, overload, TypeVar  # noqa
import datetime  # noqa: I251

from dlt.common.pendulum import pendulum, timedelta
from dlt.common.typing import TimedeltaSeconds, TAnyDateTime
from pendulum.parsing import parse_iso8601, _parse_common as parse_datetime_common
from pendulum.tz import UTC


PAST_TIMESTAMP: float = 0.0
FUTURE_TIMESTAMP: float = 9999999999.0
DAY_DURATION_SEC: float = 24 * 60 * 60.0


def timestamp_within(timestamp: float, min_exclusive: Optional[float], max_inclusive: Optional[float]) -> bool:
    """
    check if timestamp within range uniformly treating none and range inclusiveness
    """
    return timestamp > (min_exclusive or PAST_TIMESTAMP) and timestamp <= (max_inclusive or FUTURE_TIMESTAMP)


def timestamp_before(timestamp: float, max_inclusive: Optional[float]) -> bool:
    """
    check if timestamp is before max timestamp, inclusive
    """
    return timestamp <= (max_inclusive or FUTURE_TIMESTAMP)


def parse_iso_like_datetime(value: Any) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]:
    # we use internal pendulum parse function. the generic function, for example, parses string "now" as now()
    # it also tries to parse ISO intervals but the code is very low quality

    # only iso dates are allowed
    dtv = None
    with contextlib.suppress(ValueError):
        dtv = parse_iso8601(value)
    # now try to parse a set of ISO like dates
    if not dtv:
        dtv = parse_datetime_common(value)
    if isinstance(dtv, datetime.time):
        return pendulum.time(dtv.hour, dtv.minute, dtv.second, dtv.microsecond)
    if isinstance(dtv, datetime.datetime):
        return pendulum.instance(dtv)
    return pendulum.date(dtv.year, dtv.month, dtv.day)


def ensure_pendulum_date(value: TAnyDateTime) -> pendulum.Date:
    """Coerce a date/time value to a `pendulum.Date` object.

    UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A timezone aware pendulum.Date object.
    """
    if isinstance(value, datetime.datetime):
        # both py datetime and pendulum datetime are handled here
        value = pendulum.instance(value)
        return value.in_tz(UTC).date()  # type: ignore
    elif isinstance(value, datetime.date):
        return pendulum.date(value.year, value.month, value.day)
    elif isinstance(value, (int, float, str)):
        result = _datetime_from_ts_or_iso(value)
        if isinstance(result, datetime.time):
            raise ValueError(f"Cannot coerce {value} to a pendulum.DateTime object.")
        if isinstance(result, pendulum.DateTime):
            return result.in_tz(UTC).date()  # type: ignore
        return pendulum.date(result.year, result.month, result.day)
    raise TypeError(f"Cannot coerce {value} to a pendulum.DateTime object.")


def ensure_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a date/time value to a `pendulum.DateTime` object.

    UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A timezone aware pendulum.DateTime object in UTC timezone.
    """
    if isinstance(value, datetime.datetime):
        # both py datetime and pendulum datetime are handled here
        ret = pendulum.instance(value)
        return ret.in_tz(UTC)
    elif isinstance(value, datetime.date):
        return pendulum.datetime(value.year, value.month, value.day, tz=UTC)
    elif isinstance(value, (int, float, str)):
        result = _datetime_from_ts_or_iso(value)
        if isinstance(result, datetime.time):
            raise ValueError(f"Cannot coerce {value} to a pendulum.DateTime object.")
        if isinstance(result, pendulum.DateTime):
            return result.in_tz(UTC)
        return pendulum.datetime(result.year, result.month, result.day, tz=UTC)
    raise TypeError(f"Cannot coerce {value} to a pendulum.DateTime object.")


def ensure_pendulum_time(value: Union[str, datetime.time]) -> pendulum.Time:
    """Coerce a time value to a `pendulum.Time` object.

    Args:
        value: The value to coerce. Can be a `pendulum.Time` / `datetime.time` or an iso time string.

    Returns:
        A pendulum.Time object
    """

    if isinstance(value, datetime.time):
        if isinstance(value, pendulum.Time):
            return value
        return pendulum.time(value.hour, value.minute, value.second, value.microsecond)
    elif isinstance(value, str):
        result = parse_iso_like_datetime(value)
        if isinstance(result, pendulum.Time):
            return result
        else:
            raise ValueError(f"{value} is not a valid ISO time string.")
    raise TypeError(f"Cannot coerce {value} to a pendulum.Time object.")


def _datetime_from_ts_or_iso(value: Union[int, float, str]) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]:
    if isinstance(value, (int, float)):
        return pendulum.from_timestamp(value)
    try:
        return parse_iso_like_datetime(value)
    except ValueError:
        value = float(value)
        return pendulum.from_timestamp(float(value))


@overload
def to_seconds(td: None) -> None:
    pass


@overload
def to_seconds(td: TimedeltaSeconds) -> float:
    pass


def to_seconds(td: Optional[TimedeltaSeconds]) -> Optional[float]:
    if isinstance(td, timedelta):
        return td.total_seconds()
    return td


T = TypeVar("T", bound=Union[pendulum.DateTime, pendulum.Time])

def reduce_pendulum_datetime_precision(value: T, microsecond_precision: int) -> T:
    return value.replace(microsecond=value.microsecond // 10**(6 - microsecond_precision) * 10**(6 - microsecond_precision))  # type: ignore
