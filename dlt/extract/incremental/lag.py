import sys
from datetime import datetime, timedelta, date  # noqa: I251
from typing import Union

from dlt.common import logger
from dlt.common.time import (
    detect_datetime_format,
    ensure_pendulum_datetime_non_utc,
    datatime_obj_to_str,
)

from . import TCursorValue, LastValueFunc


def _apply_lag_to_value(
    lag: float, value: TCursorValue, last_value_func: LastValueFunc[TCursorValue]
) -> TCursorValue:
    """Applies lag to a value, in case of `str` types it attempts to return a string
    with the lag applied preserving original format of a datetime/date
    """
    # Determine if the input is originally a string and capture its format
    is_str = isinstance(value, str)
    value_format = detect_datetime_format(value) if is_str else None
    is_str_date = value_format in ("%Y%m%d", "%Y-%m-%d") if value_format else None
    parsed_value = ensure_pendulum_datetime_non_utc(value) if is_str else value

    if isinstance(parsed_value, (datetime, date)):
        parsed_value = _apply_lag_to_datetime(lag, parsed_value, last_value_func, is_str_date)  # type: ignore[assignment]
        # go back to string or pass exact type
        value = datatime_obj_to_str(parsed_value, value_format) if value_format else parsed_value  # type: ignore[assignment]

    elif isinstance(parsed_value, (int, float)):
        value = _apply_lag_to_number(lag, parsed_value, last_value_func)  # type: ignore[assignment]

    else:
        logger.error(
            f"Lag is not supported for cursor type: {type(value)} with last_value_func:"
            f" {last_value_func}. Strings must parse to DateTime or Date."
        )

    return value


def _apply_lag_to_datetime(
    lag: float,
    value: Union[datetime, date],
    last_value_func: LastValueFunc[TCursorValue],
    is_str_date: bool,
) -> Union[datetime, date]:
    if isinstance(value, datetime) and not is_str_date:
        delta = timedelta(seconds=lag)
    elif is_str_date or isinstance(value, date):
        delta = timedelta(days=lag)
    return value - delta if last_value_func is max else value + delta


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
) -> TCursorValue:
    """Applies lag to `last_value` but prevents it to cross `initial_value`: observing order of last_value_func"""
    # Skip lag adjustment to avoid out-of-bounds issues
    lagged_last_value = _apply_lag_to_value(lag, last_value, last_value_func)
    if (
        initial_value is not None
        and last_value_func((initial_value, lagged_last_value)) == initial_value
    ):
        # do not cross initial_value
        return initial_value
    return lagged_last_value
