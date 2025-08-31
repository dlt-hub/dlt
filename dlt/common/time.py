import contextlib
import datetime  # noqa: I251
import re
import sys
from typing import Any, NoReturn, Optional, Union, cast, overload, TypeVar, Callable
from functools import partial, singledispatch

from pendulum.parsing import (
    parse_iso8601,
    DEFAULT_OPTIONS as pendulum_options,
    _parse_common as parse_datetime_common,
)

from dlt.common.pendulum import pendulum, timedelta
from dlt.common.typing import (
    TNativeDateTime,
    TPendulumDateTime,
    TSerializedDateTime,
    TimedeltaSeconds,
    TAnyDateTime,
)

PAST_TIMESTAMP: float = 0.0
FUTURE_TIMESTAMP: float = 9999999999.0
DAY_DURATION_SEC: float = 24 * 60 * 60.0

precise_time: Callable[[], float] = None
"""A precise timer using win_precise_time library on windows and time.time on other systems"""

try:
    import win_precise_time as wpt

    precise_time = wpt.time
except ImportError:
    from time import time as _built_in_time

    precise_time = _built_in_time


def timestamp_within(
    timestamp: float, min_exclusive: Optional[float], max_inclusive: Optional[float]
) -> bool:
    """
    check if timestamp within range uniformly treating none and range inclusiveness
    """
    return timestamp > (min_exclusive or PAST_TIMESTAMP) and timestamp <= (
        max_inclusive or FUTURE_TIMESTAMP
    )


def timestamp_before(timestamp: float, max_inclusive: Optional[float]) -> bool:
    """
    check if timestamp is before max timestamp, inclusive
    """
    return timestamp <= (max_inclusive or FUTURE_TIMESTAMP)


def parse_iso_like_datetime(value: Any) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]:
    """Parses ISO8601 string into pendulum datetime, date or time. Preserves timezone info.
    Note: naive datetimes will be generated from string without timezone

       we use internal pendulum parse function. the generic function, for example, parses string "now" as now()
       it also tries to parse ISO intervals but the code is very low quality
    """
    # only iso dates are allowed
    dtv: Union[
        TPendulumDateTime, TNativeDateTime, pendulum.Duration, pendulum.Time, datetime.time
    ] = None
    with contextlib.suppress(ValueError):
        dtv = cast(Union[TNativeDateTime, pendulum.Duration], parse_iso8601(value))
    # now try to parse a set of ISO like dates
    if not dtv:
        dtv = cast(
            Union[datetime.datetime, datetime.date, datetime.time],
            parse_datetime_common(value, **pendulum_options),
        )
    if isinstance(dtv, datetime.time):
        return pendulum.time(dtv.hour, dtv.minute, dtv.second, dtv.microsecond)
    if isinstance(dtv, datetime.datetime):
        return pendulum.instance(dtv, tz=dtv.tzinfo)
    if isinstance(dtv, pendulum.Duration):
        raise ValueError(f"Interval ISO 8601 not supported: `{value}`")
    return pendulum.date(dtv.year, dtv.month, dtv.day)


def ensure_pendulum_date(value: TAnyDateTime) -> pendulum.Date:
    """Coerce a date/time value to a `pendulum.Date` object.

    UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A timezone aware pendulum.Date object.
    """

    return ensure_pendulum_datetime(value).date()


def ensure_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a date/time value to a `pendulum.DateTime` object.

    UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A timezone aware pendulum.DateTime object in UTC timezone.
    """

    return ensure_pendulum_datetime_non_utc(value).in_tz(pendulum.UTC)


def ensure_pendulum_datetime_non_utc(value: TAnyDateTime) -> pendulum.DateTime:
    """
    Ensure the value is a naive pendulum.DateTime object.
    """

    result: Union[pendulum.DateTime, pendulum.Date, pendulum.Time, datetime.datetime, datetime.date]

    try:
        result = parse_dt(cast(TSerializedDateTime, value))
    except ValueError:
        result = cast(Union[datetime.datetime, datetime.date], value)

    if isinstance(result, datetime.datetime):
        result = pendulum.instance(result, tz=result.tzinfo)
    elif isinstance(result, (pendulum.Date, datetime.date)):
        result = pendulum.datetime(result.year, result.month, result.day)

    if not isinstance(result, pendulum.DateTime):
        # time/duration objects are not supported
        raise ValueError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")

    return result


def datatime_obj_to_str(
    datatime: Union[datetime.datetime, datetime.date], datetime_format: str
) -> str:
    if sys.version_info < (3, 12, 0) and "%:z" in datetime_format:
        modified_format = datetime_format.replace("%:z", "%z")
        datetime_str = datatime.strftime(modified_format)

        timezone_part = datetime_str[-5:] if len(datetime_str) >= 5 else ""
        if timezone_part.startswith(("-", "+")):
            return f"{datetime_str[:-5]}{timezone_part[:3]}:{timezone_part[3:]}"

        raise ValueError(f"Invalid timezone format in datetime string: `{datetime_str}`")

    return datatime.strftime(datetime_format)


def ensure_pendulum_time(
    value: Union[TSerializedDateTime, datetime.time, timedelta]
) -> pendulum.Time:
    """Coerce a time value to a `pendulum.Time` object.

    Args:
        value: The value to coerce. Can be a `pendulum.Time` / `datetime.time` or an iso time string.

    Returns:
        A pendulum.Time object
    """

    result: Union[pendulum.Time, pendulum.Duration, datetime.time, timedelta]

    try:
        # Try to parse the value as a serialized datetime/date/time/interval
        result = cast(
            Union[pendulum.Time, pendulum.Duration], parse_dt(cast(TSerializedDateTime, value))
        )
    except ValueError:
        result = cast(Union[datetime.time, timedelta], value)

    if isinstance(result, datetime.time):
        result = pendulum.instance(result)
    elif isinstance(result, (timedelta, pendulum.Duration)):
        # Assume timedelta is seconds passed since midnight. Some drivers (mysqlclient) return time in this format
        result = pendulum.time(
            result.seconds // 3600,
            (result.seconds // 60) % 60,
            result.seconds % 60,
            result.microseconds,
        )

    if not isinstance(result, pendulum.Time):
        raise TypeError(f"Cannot coerce `{value}` to `pendulum.Time` object.")

    return result


def detect_datetime_format(value: str) -> Optional[str]:
    format_patterns = {
        # Full datetime with 'Z' (UTC) or timezone offset
        re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"): "%Y-%m-%dT%H:%M:%SZ",  # UTC 'Z'
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$"
        ): "%Y-%m-%dT%H:%M:%S.%fZ",  # UTC with fractional seconds
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$"
        ): "%Y-%m-%dT%H:%M:%S%:z",  # Positive timezone offset
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}$"
        ): "%Y-%m-%dT%H:%M:%S%z",  # Positive timezone without colon
        # Full datetime with fractional seconds and positive timezone offset
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}$"
        ): "%Y-%m-%dT%H:%M:%S.%f%:z",
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{4}$"
        ): "%Y-%m-%dT%H:%M:%S.%f%z",  # Positive timezone without colon
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-\d{2}:\d{2}$"
        ): "%Y-%m-%dT%H:%M:%S%:z",  # Negative timezone offset
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-\d{4}$"
        ): "%Y-%m-%dT%H:%M:%S%z",  # Negative timezone without colon
        # Full datetime with fractional seconds and negative timezone offset
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+-\d{2}:\d{2}$"
        ): "%Y-%m-%dT%H:%M:%S.%f%:z",
        re.compile(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+-\d{4}$"
        ): "%Y-%m-%dT%H:%M:%S.%f%z",  # Negative Timezone without colon
        # Datetime without timezone
        re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"): "%Y-%m-%dT%H:%M:%S",  # No timezone
        re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$"): "%Y-%m-%dT%H:%M",  # Minute precision
        re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}$"): "%Y-%m-%dT%H",  # Hour precision
        # Date-only formats
        re.compile(r"^\d{4}-\d{2}-\d{2}$"): "%Y-%m-%d",  # Date only
        re.compile(r"^\d{4}-\d{2}$"): "%Y-%m",  # Year and month
        re.compile(r"^\d{4}$"): "%Y",  # Year only
        # Week-based date formats
        re.compile(r"^\d{4}-W\d{2}$"): "%Y-W%W",  # Week-based date
        re.compile(r"^\d{4}-W\d{2}-\d{1}$"): "%Y-W%W-%u",  # Week-based date with day
        # Ordinal date formats (day of year)
        re.compile(r"^\d{4}-\d{3}$"): "%Y-%j",  # Ordinal date
        # Compact formats (no dashes)
        re.compile(r"^\d{8}$"): "%Y%m%d",  # Compact date format
        re.compile(r"^\d{6}$"): "%Y%m",  # Compact year and month format
    }

    # Match against each compiled regular expression
    for pattern, format_str in format_patterns.items():
        if pattern.match(value):
            return format_str

    # Return None if no pattern matches
    return None


def to_py_datetime(value: datetime.datetime) -> datetime.datetime:
    """Convert a pendulum.DateTime to a py datetime object.

    Args:
        value: The value to convert. Can be a pendulum.DateTime or datetime.

    Returns:
        A py datetime object
    """
    if isinstance(value, pendulum.DateTime):
        return datetime.datetime(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            value.tzinfo,
        )
    return value


def to_py_date(value: datetime.date) -> datetime.date:
    """Convert a pendulum.Date to a py date object.

    Args:
        value: The value to convert. Can be a pendulum.Date or date.

    Returns:
        A py date object
    """
    if isinstance(value, pendulum.Date):
        return datetime.date(value.year, value.month, value.day)
    return value


def datetime_to_timestamp(moment: Union[datetime.datetime, pendulum.DateTime]) -> int:
    return int(moment.timestamp())


def datetime_to_timestamp_ms(moment: Union[datetime.datetime, pendulum.DateTime]) -> int:
    return int(moment.timestamp() * 1000)


@overload
def parse_dt(value: Union[float, int]) -> pendulum.DateTime:
    pass


@overload
def parse_dt(value: str) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]:
    pass


@singledispatch
def parse_dt(value: Any) -> NoReturn:
    raise ValueError("Can not coerce `None` to pendulum compatible types")


@parse_dt.register(float)  # type: ignore[attr-defined]
@parse_dt.register(int)  # type: ignore[attr-defined]
def _(value: Union[float, int]) -> pendulum.DateTime:
    return pendulum.from_timestamp(value, pendulum.UTC)


@parse_dt.register(str)  # type: ignore[attr-defined]
def _(value: str) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time, pendulum.Duration]:
    """A helper function to parse datetime strings serialized in ISO/RFC formats"""

    normalized_dt: Union[pendulum.DateTime, pendulum.Date, pendulum.Time, pendulum.Duration] = None

    for parser in (
        parse_iso_like_datetime,
        partial(pendulum.parse, exact=True, strict=False),
    ):
        with contextlib.suppress(ValueError):
            normalized_dt = parser(value)
            break

    if not normalized_dt:
        raise ValueError(f"Cannot parse a datetime value from `{value}`")

    return normalized_dt


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


TTimeWithPrecision = TypeVar("TTimeWithPrecision", bound=Union[pendulum.DateTime, pendulum.Time])


def reduce_pendulum_datetime_precision(
    value: TTimeWithPrecision, precision: int
) -> TTimeWithPrecision:
    if precision >= 6:
        return value
    return value.replace(microsecond=value.microsecond // 10 ** (6 - precision) * 10 ** (6 - precision))  # type: ignore
