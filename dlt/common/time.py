import contextlib
import datetime  # noqa: I251
import re
import sys
from typing import Any, Optional, Union, overload, TypeVar, Callable  # noqa

from pendulum.parsing import (
    parse_iso8601,
    DEFAULT_OPTIONS as pendulum_options,
    _parse_common as parse_datetime_common,
)
from pendulum.tz import UTC
from pendulum import DateTime, Date, Time  # noqa: I251

from dlt.common.pendulum import create_dt, pendulum, timedelta
from dlt.common.typing import TimedeltaSeconds, TAnyDateTime
from dlt.common.warnings import deprecated

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


def parse_iso_like_datetime(value: str) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]:
    """Parses ISO8601 string into pendulum datetime, date or time. Preserves timezone info.
    Note: naive datetimes will be generated from string without timezone

       we use internal pendulum parse function. the generic function, for example, parses string "now" as now()
       it also tries to parse ISO intervals but the code is very low quality
    """
    # only iso dates are allowed
    dtv = None
    with contextlib.suppress(ValueError):
        dtv = parse_iso8601(value)
    # now try to parse a set of ISO like dates
    if not dtv:
        dtv = parse_datetime_common(value, **pendulum_options)
    # this is what pendulum.instance does but datetime is checked first
    if isinstance(dtv, datetime.datetime):
        return create_dt(
            dtv.year,
            dtv.month,
            dtv.day,
            dtv.hour,
            dtv.minute,
            dtv.second,
            dtv.microsecond,
            tz=dtv.tzinfo,
            fold=dtv.fold,
        )
    if isinstance(dtv, datetime.date):
        return Date(dtv.year, dtv.month, dtv.day)
    if isinstance(dtv, datetime.time):
        # NOTE: Time disregards timezones on `add` and `subtract`
        # TODO: we are better off switching to regular datetime.time
        return Time(
            dtv.hour,
            dtv.minute,
            dtv.second,
            dtv.microsecond,
            tzinfo=dtv.tzinfo,
            fold=dtv.fold,
        )

    raise ValueError(f"Interval ISO 8601 not supported: `{value}`")


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
        value = create_dt(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tz=value.tzinfo,
            fold=value.fold,
        )
        return value.in_tz(UTC).date()
    elif isinstance(value, datetime.date):
        return Date(value.year, value.month, value.day)
    elif isinstance(value, (int, float, str)):
        result = _datetime_from_ts_or_iso(value)
        if isinstance(result, datetime.time):
            raise ValueError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")
        if isinstance(result, pendulum.DateTime):
            return result.in_tz(UTC).date()
        return pendulum.date(result.year, result.month, result.day)
    raise TypeError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")


def ensure_pendulum_datetime_utc(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a date/time value to a `pendulum.DateTime` object.

    UTC is assumed if the value is not timezone aware. Other timezones are shifted to UTC

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A timezone aware pendulum.DateTime object in UTC timezone.
    """
    return ensure_pendulum_datetime_non_utc(value).in_tz(UTC)


ensure_pendulum_datetime = deprecated("Use ensure_pendulum_datetime_utc instead")(
    ensure_pendulum_datetime_utc
)


def ensure_pendulum_datetime_non_utc(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a date/time value to a `pendulum.DateTime` object.

    Tz-awareness is preserved. Naive datetimes remain naive. Tz-aware datetimes keep their original timezone.
    Dates are converted to naive datetimes as dates are naive

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        pendulum.DateTime object that preserver original timezone
    """
    if isinstance(value, datetime.datetime):
        # both py datetime and pendulum datetime are handled here
        # pendulum.instance assigns UTC by default. tz=None will keep naive datetime on datetime.datetime
        return create_dt(
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            tz=value.tzinfo,
            fold=value.fold,
        )
    elif isinstance(value, datetime.date):
        return create_dt(value.year, value.month, value.day, tz=None)
    elif isinstance(value, (int, float, str)):
        result = _datetime_from_ts_or_iso(value)
        if isinstance(result, datetime.time):
            raise ValueError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")
        if isinstance(result, pendulum.DateTime):
            return result
        # naive datetime from date
        return create_dt(result.year, result.month, result.day, tz=None)
    raise TypeError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")


def normalize_timezone(dt: pendulum.DateTime, timezone: bool) -> pendulum.DateTime:
    """Normalizes timezone in a pendulum instance according to dlt convention:
    * naive datetimes represent UTC (system timezone is ignored) time zone
    * tz-aware datetimes are always UTC

    Following conversions will be made:
    * when `timezone` is false: tz-aware tz is converted into UTC tz and then naive
    * when `timezone` is true: naive and aware datetimes are converted to UTC

    """
    if timezone:
        # adds UTC to naive timezones (disregard system timezone), shifts tz aware timezones
        return dt.in_tz(UTC)
    elif dt.tzinfo is not None:
        # if tz-aware then shift to UTC first, naive() just strips tz
        return dt.in_tz(tz=UTC).naive()
    return dt


def datetime_obj_to_str(
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


def ensure_pendulum_time(value: Union[str, int, float, datetime.time, timedelta]) -> pendulum.Time:
    """Coerce a time-like value to a `pendulum.Time` object using timezone=False semantics.

    this follows normalize_timezone(..., timezone=False): tz-aware inputs are converted to UTC
    and then made naive; naive values are treated as UTC and kept naive.

    Args:
        value: Time value to coerce. Supported types:
            - pendulum.Time or datetime.time
            - ISO time string (e.g. "12:34:56", "12:34:56+02:00")
            - timedelta representing seconds since midnight

    Returns:
        A naive pendulum.Time object that represents UTC time-of-day.
    """

    def _normalize_aware_time(t: datetime.time) -> pendulum.Time:
        # fast path: if naive, do not normalize
        if t.tzinfo is None:
            return pendulum.time(t.hour, t.minute, t.second, t.microsecond)
        # build a dummy date to apply timezone normalization uniformly
        fold = getattr(t, "fold", 0)
        dt = create_dt(
            1970, 1, 1, t.hour, t.minute, t.second, t.microsecond, tz=t.tzinfo, fold=fold
        )
        ndt = normalize_timezone(dt, timezone=False)
        return pendulum.time(ndt.hour, ndt.minute, ndt.second, ndt.microsecond)

    if isinstance(value, datetime.time):
        # handles both python datetime.time and pendulum.Time
        return _normalize_aware_time(value)

    if isinstance(value, str):
        parsed = parse_iso_like_datetime(value)
        if isinstance(parsed, pendulum.Time):
            return _normalize_aware_time(parsed)
        # only ISO time strings are accepted
        raise ValueError(f"Invalid ISO time string: `{value}`")

    if isinstance(value, timedelta):
        # assume timedelta is seconds passed since midnight (eg. mysqlclient returns that)
        return pendulum.time(
            value.seconds // 3600,
            (value.seconds // 60) % 60,
            value.seconds % 60,
            value.microseconds,
        )
    raise TypeError(f"Cannot coerce `{value}` to `pendulum.Time` object.")


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


def _datetime_from_ts_or_iso(
    value: Union[int, float, str]
) -> Union[pendulum.DateTime, pendulum.Date, pendulum.Time]:
    if isinstance(value, (int, float)):
        return pendulum.from_timestamp(value)
    try:
        return parse_iso_like_datetime(value)
    except ValueError as outer_ex:
        try:
            value = float(value)
        except ValueError:
            raise outer_ex from None
        return pendulum.from_timestamp(value)


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


def get_precision_from_datetime_unit(unit: str) -> int:
    """Convert PyArrow datetime unit to numeric precision.

    Args:
        unit: PyArrow datetime unit ("s", "ms", "us", "ns")

    Returns:
        Numeric precision (0, 3, 6, or 9)
    """
    if unit == "s":
        return 0
    elif unit == "ms":
        return 3
    elif unit == "us":
        return 6
    else:  # "ns" or any other unit defaults to nanosecond precision
        return 9
