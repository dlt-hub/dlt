import datetime
import math
import os
import re
import sys
import warnings
from typing import Any, Optional, Union, overload, TypeVar, Callable  # noqa
from zoneinfo import ZoneInfo

from pendulum.parsing import (
    parse_iso8601,
    DEFAULT_OPTIONS as pendulum_options,
    _parse_common as parse_datetime_common,
)
from pendulum.tz import UTC
from pendulum import DateTime, Date, Time  # noqa: I251

from dlt.common import known_env
from dlt.common.pendulum import create_dt, ensure_pendulum_dt, pendulum, timedelta
from dlt.common.typing import TimedeltaSeconds, TAnyDateTime
from dlt.common.warnings import deprecated

PAST_TIMESTAMP: float = 0.0
FUTURE_TIMESTAMP: float = 9999999999.0
DAY_DURATION_SEC: float = 24 * 60 * 60.0
UNIX_EPOCH_DATE = datetime.date(1970, 1, 1)
UNIX_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

UTC_NAME = "UTC"

DEFAULT_TIMESTAMP_PRECISION = 6


def _env_context_timezone() -> datetime.tzinfo:
    """Timezone a launcher put in the environment, UTC when there is none or it is unusable."""
    # read on import and on every reset, so a job takes the timezone it declared without a context
    name = os.environ.get(known_env.DLT_INTERVAL_TIMEZONE)
    if not name or name == UTC_NAME:
        return datetime.timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        warnings.warn(
            f"`{known_env.DLT_INTERVAL_TIMEZONE}` is `{name}`, which is not an IANA timezone name."
            " dlt keeps storing values in UTC.",
            stacklevel=2,
        )
        return datetime.timezone.utc


_CONTEXT_TZ: datetime.tzinfo = _env_context_timezone()


def get_context_timezone() -> datetime.tzinfo:
    """Context timezone, the one `dlt` uses to normalize timestamps and to store in load packages"""

    return _CONTEXT_TZ


def to_iana_name(tz: Optional[datetime.tzinfo]) -> Optional[str]:
    """IANA name of `tz`, or `None` when it carries none, as a fixed offset does."""
    if tz is None:
        return None
    if isinstance(tz, datetime.timezone):
        # a stdlib fixed offset carries no name, and only zero offset has a portable one
        return UTC_NAME if not tz.utcoffset(None) else None
    # `key` is zoneinfo, `zone` is pytz, `name` is pendulum. a fixed offset reports its own
    # offset (`+02:00`), which neither arrow nor zoneinfo resolves
    for attr in ("key", "zone", "name"):
        if name := getattr(tz, attr, None):
            return None if name[0] in "+-" else str(name)
    return None


def get_context_timezone_name() -> str:
    """IANA name of the context timezone.

    Raises:
        ValueError: The context timezone carries no IANA name.
    """
    name = to_iana_name(_CONTEXT_TZ)
    if name is None:
        raise ValueError(
            f"The context timezone `{_CONTEXT_TZ}` has no IANA name, so `dlt` cannot label values"
            " with it. Set a canonical IANA name, for example `Europe/Berlin` or `UTC`."
        )
    return name


def set_context_timezone(tz: Optional[datetime.tzinfo]) -> datetime.tzinfo:
    """Installs the context timezone and returns the previous one.

    Internal: called by `TimezoneContext` lifecycle hooks and by a launcher preparing a run.

    Args:
        tz: Timezone to install, `None` to fall back on the environment, itself UTC by default.

    Returns:
        The timezone that was installed before this call.
    """
    global _CONTEXT_TZ

    previous = _CONTEXT_TZ
    _CONTEXT_TZ = tz or _env_context_timezone()
    return previous


precise_time: Callable[[], float] = None
"""A precise timer using win_precise_time library on windows and time.time on other systems"""

try:
    import win_precise_time as wpt

    precise_time = wpt.time
except ImportError:
    from time import time as _built_in_time

    precise_time = _built_in_time


class MonotonicPreciseTime:
    """Wall-clock timer guaranteed to never go backward.

    Reads wall clock on every call and tracks the highest value seen.  When the
    wall clock jumps backward (NTP step corrections, VM/WSL clock drift) the
    previous high-water mark is returned instead.

    Args:
        strictly_increasing: every call returns a value strictly greater than
            the previous one by bumping with ``math.nextafter`` when the wall
            clock does not advance. Use for callers that need unique timestamps.

    Not thread-safe. Use ``LockedMonotonicPreciseTime`` for shared instances.
    """

    def __init__(self, strictly_increasing: bool = False) -> None:
        self._last: float = precise_time()
        self._strictly_increasing = strictly_increasing

    def __call__(self) -> float:
        wall = precise_time()
        if wall > self._last:
            self._last = wall
        elif self._strictly_increasing:
            self._last = math.nextafter(self._last, math.inf)
        return self._last


class LockedMonotonicPreciseTime(MonotonicPreciseTime):
    """Thread-safe variant using a lock (one uncontended futex CAS)."""

    def __init__(self, strictly_increasing: bool = False) -> None:
        import threading

        super().__init__(strictly_increasing=strictly_increasing)
        self._lock = threading.Lock()

    def __call__(self) -> float:
        wall = precise_time()
        with self._lock:
            if wall > self._last:
                self._last = wall
            elif self._strictly_increasing:
                self._last = math.nextafter(self._last, math.inf)
            return self._last


increasing_precise_time = LockedMonotonicPreciseTime(strictly_increasing=True)


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


def parse_iso_like_datetime(value: str) -> Union[DateTime, Date, Time]:
    """Parses ISO8601 string into pendulum datetime, date or time. Preserves timezone info.

    Note: naive datetimes will be generated from string without timezone.
    """
    try:
        dtv = parse_iso8601(value)
    except ValueError:
        # fallback for formats like "2024" (year only) or "2024/01/15"
        dtv = parse_datetime_common(value, **pendulum_options)

    if isinstance(dtv, datetime.datetime):
        return ensure_pendulum_dt(dtv)
    if isinstance(dtv, datetime.date):
        return Date(dtv.year, dtv.month, dtv.day)
    if isinstance(dtv, datetime.time):
        return Time(
            dtv.hour,
            dtv.minute,
            dtv.second,
            dtv.microsecond,
            tzinfo=dtv.tzinfo,
            fold=dtv.fold,
        )
    raise ValueError(f"Interval ISO 8601 not supported: `{value}`")


def ensure_pendulum_date(
    value: TAnyDateTime, tz: Optional[datetime.tzinfo] = None
) -> pendulum.Date:
    """Coerce a date/time value to the `pendulum.Date` it falls on in `tz`.

    A naive value is taken to already be in `tz`, an aware value is converted to `tz` first.

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
        tz: Timezone the day is taken in. Defaults to the context timezone, itself UTC unless
            a `TimezoneContext` is active.

    Returns:
        A pendulum.Date object.
    """
    d = ensure_date(value, tz)
    return pendulum.Date(d.year, d.month, d.day)


def ensure_pendulum_datetime(
    value: TAnyDateTime, tz: Optional[datetime.tzinfo] = None
) -> pendulum.DateTime:
    """Coerce a date/time value to a tz-aware `pendulum.DateTime` in `tz`.

    The `pendulum` counterpart of `ensure_datetime_in_tz`.

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
        tz: Timezone to put the value in. Defaults to the context timezone, itself UTC unless a
            `TimezoneContext` is active.

    Returns:
        A timezone aware pendulum.DateTime object.
    """
    return ensure_pendulum_dt(ensure_datetime_in_tz(value, tz))


def ensure_datetime(value: TAnyDateTime) -> datetime.datetime:
    """Coerce a date/time value to a stdlib `datetime.datetime`, preserving original timezone.

    Tz-awareness is preserved. Naive datetimes remain naive. Tz-aware datetimes keep their original timezone.

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A stdlib `datetime.datetime` that preserves original timezone.
    """
    if isinstance(value, datetime.datetime):
        # no pendulum round-trip: the tzinfo the caller passed is kept as-is
        return to_py_datetime(value)
    return to_py_datetime(_parse_pendulum_datetime(value))


def ensure_datetime_in_tz(
    value: TAnyDateTime, tz: Optional[datetime.tzinfo] = None
) -> datetime.datetime:
    """Coerce a date/time value to a tz-aware stdlib `datetime.datetime` in `tz`.

    A naive input is taken to already be in `tz`, so the system timezone never takes part. An
    aware input is converted to `tz`, keeping its instant.

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
        tz: Target timezone. Defaults to the context timezone, itself UTC unless a
            `TimezoneContext` is active.

    Returns:
        A stdlib `datetime.datetime` with `tzinfo == tz`.
    """
    return normalize_timezone(ensure_datetime(value), True, tz)


def ensure_date(value: TAnyDateTime, tz: Optional[datetime.tzinfo] = None) -> datetime.date:
    """Coerce a date/time value to the calendar day it falls on in `tz`.

    A naive value is taken to already be in `tz`, an aware value is converted to `tz` first, so
    the same instant can be a different day in a different timezone.

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.
        tz: Timezone the day is taken in. Defaults to the configured timezone, itself UTC unless
            a `TimezoneContext` is active.

    Returns:
        A stdlib `datetime.date`.
    """
    return ensure_datetime_in_tz(value, tz).date()


def _parse_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a date/time value to a `pendulum.DateTime` object.

    Tz-awareness is preserved. Naive datetimes remain naive. Tz-aware datetimes keep their original timezone.
    Dates are converted to naive datetimes as dates are naive

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        pendulum.DateTime object that preserver original timezone
    """
    if isinstance(value, str):
        # fast path for ISO datetime strings
        try:
            dtv = parse_iso8601(value)
            if isinstance(dtv, datetime.datetime):
                return ensure_pendulum_dt(dtv)
        except ValueError:
            pass

    if isinstance(value, datetime.datetime):
        return ensure_pendulum_dt(value)
    elif isinstance(value, datetime.date):
        return DateTime(value.year, value.month, value.day)
    elif isinstance(value, (int, float, str)):
        result = _datetime_from_ts_or_iso(value)
        if isinstance(result, datetime.time):
            raise ValueError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")
        if isinstance(result, pendulum.DateTime):
            return result
        # naive datetime from date
        return DateTime(result.year, result.month, result.day)
    raise TypeError(f"Cannot coerce `{value}` to `pendulum.DateTime` object.")


def normalize_timezone(
    value: datetime.datetime, timezone: bool, tz: Optional[datetime.tzinfo] = None
) -> datetime.datetime:
    """Puts a datetime in the context timezone, per the `timezone` column hint.

    A naive input is taken to already be in `tz`, so the system timezone never takes part.

    Args:
        value: An already parsed datetime. This runs per value while normalizing, so it does not
            coerce - call `ensure_datetime_in_tz` for anything else.
        timezone: The column's `timezone` hint. `False` returns a naive value.
        tz: Timezone to put the value in. Defaults to the context timezone, itself UTC unless a
            `TimezoneContext` is active.

    Returns:
        A tz-aware datetime in `tz`, naive when `timezone` is `False`.
    """
    value_tz = value.tzinfo
    if value_tz is None and not timezone:
        # nothing to convert and no zone to strip
        return value
    if tz is None:
        tz = _CONTEXT_TZ
    if value_tz is None:
        # a naive value already counts as being in `tz`
        return value.replace(tzinfo=tz)
    if value_tz is not tz:
        # pendulum converts several times slower than the stdlib
        if isinstance(value, DateTime):
            value = to_py_datetime(value)
        value = value.astimezone(tz)
    return value if timezone else value.replace(tzinfo=None)


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


def date_to_epoch_days(value: datetime.date) -> int:
    """Converts date value to number of days since Unix epoch."""
    return value.toordinal() - UNIX_EPOCH_DATE.toordinal()


_PERIOD_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_PERIOD_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$")


def parse_period_seconds(value: str) -> float:
    """Parse a human period string (e.g. '5m', '1h', '30s') into seconds.

    Also accepts bare numeric strings as seconds.

    Raises:
        ValueError: If the string cannot be parsed.
    """
    match = _PERIOD_RE.match(value.strip())
    if match:
        return float(match.group(1)) * _PERIOD_MULTIPLIERS[match.group(2)]
    return float(value)


def ensure_pendulum_time(value: Union[str, int, float, datetime.time, timedelta]) -> pendulum.Time:
    """Coerce a time-like value to a `pendulum.Time` object using timezone=False semantics.

    Follows `normalize_timezone(..., timezone=False)`: an aware input is converted to the
    configured timezone and then made naive, a naive value is kept as it is.

    Args:
        value: Time value to coerce. Supported types:
            - pendulum.Time or datetime.time
            - ISO time string (e.g. "12:34:56", "12:34:56+02:00")
            - timedelta representing seconds since midnight

    Returns:
        A naive pendulum.Time object, its time-of-day in the configured timezone.
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
        # Week-based date formats (ISO 8601: week-numbering year %G + ISO week %V)
        re.compile(r"^\d{4}-W\d{2}$"): "%G-W%V",  # Week-based date
        re.compile(r"^\d{4}-W\d{2}-\d{1}$"): "%G-W%V-%u",  # Week-based date with day
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


def datetime_to_timestamp(moment: datetime.datetime) -> int:
    """Converts a datetime to whole seconds since Unix epoch, naive input taken as UTC."""
    return _epoch_delta(moment) // datetime.timedelta(seconds=1)


def datetime_to_timestamp_ms(moment: datetime.datetime) -> int:
    """Converts a datetime to whole milliseconds since Unix epoch, naive input taken as UTC."""
    return _epoch_delta(moment) // datetime.timedelta(milliseconds=1)


def datetime_to_timestamp_us(moment: datetime.datetime) -> int:
    """Converts a datetime to whole microseconds since Unix epoch, naive input taken as UTC."""
    return _epoch_delta(moment) // datetime.timedelta(microseconds=1)


def _epoch_delta(moment: datetime.datetime) -> datetime.timedelta:
    # `timestamp()` would read a naive value in the machine timezone, dlt takes it as UTC.
    # `to_py_datetime` because subtracting pendulum instances yields an `Interval`, which
    # floor-divides through a lossy `Duration`
    py_moment = to_py_datetime(moment)
    if py_moment.tzinfo is None:
        py_moment = py_moment.replace(tzinfo=datetime.timezone.utc)
    return py_moment - UNIX_EPOCH


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


TTimeWithPrecision = TypeVar("TTimeWithPrecision", bound=Union[datetime.datetime, datetime.time])


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
