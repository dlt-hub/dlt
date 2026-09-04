from datetime import datetime, timedelta, date, timezone  # noqa: I251
from typing import Any, Optional, Type, Union

from dlt.common import logger
from dlt.common.time import (
    detect_datetime_format,
    ensure_datetime,
    ensure_date,
    datetime_obj_to_str,
)
from dlt.common.typing import is_subclass

from . import TCursorValue, LastValueFunc

DATE_STR_FORMATS = ("%Y%m%d", "%Y-%m-%d")
"""Detected formats of string cursors that are dates and not datetimes"""


def _cursor_date_type(cursor_type: Optional[Type[Any]], initial_value: Any) -> Optional[Type[Any]]:
    """Tells if the declared cursor is a `date` or a `datetime`, None if it is neither."""
    if is_subclass(cursor_type, datetime):
        return datetime
    if is_subclass(cursor_type, date):
        return date
    if isinstance(initial_value, str):
        if value_format := detect_datetime_format(initial_value):
            return date if value_format in DATE_STR_FORMATS else datetime
    return None


def _apply_lag_to_value(
    lag: float,
    value: Any,
    last_value_func: LastValueFunc[TCursorValue],
    date_type: Optional[Type[Any]] = None,
) -> Any:
    """Applies lag to a value, in case of `str` types it attempts to return a string
    with the lag applied preserving original format of a datetime/date
    """
    value_format: str = None
    if isinstance(value, str):
        value_format = detect_datetime_format(value)
        if date_type is None:
            date_type = date if value_format in DATE_STR_FORMATS else datetime
        elif "%H" in (value_format or "") and not is_subclass(date_type, datetime):
            # a date has no time part to render back into the original format
            value_format = "%Y-%m-%d"
    elif isinstance(value, date) and date_type is None:
        date_type = datetime if isinstance(value, datetime) else date

    if isinstance(value, (str, date)):
        # stdlib types only, pendulum arithmetic drops any tzinfo that is not its own
        value = ensure_datetime(value) if is_subclass(date_type, datetime) else ensure_date(value)
        value = _apply_lag_to_datetime(lag, value, last_value_func)
        # go back to string or pass exact type
        value = datetime_obj_to_str(value, value_format) if value_format else value

    elif isinstance(value, (int, float)):
        value = _apply_lag_to_number(lag, value, last_value_func)

    else:
        raise ValueError(
            value,
            f"Lag is not supported for cursor type: {type(value)} with last_value_func:"
            f" {last_value_func}. Strings must parse to DateTime or Date.",
        )

    return value


def _apply_lag_to_datetime(
    lag: float,
    value: Union[date, datetime],
    last_value_func: LastValueFunc[TCursorValue],
) -> Union[date, datetime]:
    if last_value_func is max:
        lag = -lag

    if not isinstance(value, datetime):
        return value + timedelta(days=lag)
    if value.tzinfo is None:
        return value + timedelta(seconds=lag)
    # shift the instant and not the wall clock so the lag stays exact across DST transitions
    return (value.astimezone(timezone.utc) + timedelta(seconds=lag)).astimezone(value.tzinfo)


def _apply_lag_to_number(
    lag: float, value: Union[int, float], last_value_func: LastValueFunc[TCursorValue]
) -> Union[int, float]:
    adjusted_value = value - lag if last_value_func is max else value + lag
    return int(adjusted_value) if isinstance(value, int) else adjusted_value


def apply_lag(
    lag: float,
    initial_value: TCursorValue,
    last_value: TCursorValue,
    last_value_func: LastValueFunc[TCursorValue],
    cursor_type: Optional[Type[Any]] = None,
) -> TCursorValue:
    """Applies lag to `last_value` but prevents it to cross `initial_value`: observing order of last_value_func

    Lag is in days for a `date` cursor and in seconds for a `datetime` one, as declared by `cursor_type`.
    """
    lagged_last_value = _apply_lag_to_value(
        lag, last_value, last_value_func, _cursor_date_type(cursor_type, initial_value)
    )
    if (
        initial_value is not None
        and last_value_func((initial_value, lagged_last_value)) == initial_value
    ):
        # do not cross initial_value
        return initial_value
    return lagged_last_value  # type: ignore[no-any-return]


def apply_lag_with_suppression(
    lag: Optional[float],
    last_value_func: LastValueFunc[TCursorValue],
    initial_value: Optional[TCursorValue],
    end_value: Optional[TCursorValue],
    last_value: Optional[TCursorValue],
    resource_name: Optional[str] = None,
    cursor_type: Optional[Type[Any]] = None,
) -> Optional[TCursorValue]:
    """Conditionally apply lag to `last_value`, mirroring `Incremental.last_value` rules.

    Returns `last_value` unchanged when:
    - `lag` is falsy or `last_value` is None
    - `last_value_func` is not `max` or `min` (logs warning)
    - `end_value` is set (lag auto-deactivated; logs info)
    """
    if not lag or last_value is None:
        return last_value
    if last_value_func not in (max, min):
        logger.warning(
            f"Lag on {resource_name} is only supported for max or min last_value_func."
            f" Provided: {last_value_func}"
        )
        return last_value
    if end_value is not None:
        logger.info(f"Lag on {resource_name} is deactivated if end_value is set in incremental.")
        return last_value
    return apply_lag(lag, initial_value, last_value, last_value_func, cursor_type)
