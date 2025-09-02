from datetime import datetime, timedelta, date  # noqa: I251
from typing import Any, Union

from dlt.common import logger, pendulum
from dlt.common.time import (
    detect_datetime_format,
    ensure_pendulum_date,
    ensure_pendulum_datetime_non_utc,
    datetime_obj_to_str,
)

from . import TCursorValue, LastValueFunc


def _apply_lag_to_value(
    lag: float, value: Any, last_value_func: LastValueFunc[TCursorValue]
) -> Any:
    """Applies lag to a value, in case of `str` types it attempts to return a string
    with the lag applied preserving original format of a datetime/date
    """
    # Determine if the input is originally a string and capture its format
    value_format: str = None
    if isinstance(value, str):
        value_format = detect_datetime_format(value)
        is_str_date = value_format in ("%Y%m%d", "%Y-%m-%d")
        value = (
            ensure_pendulum_date(value) if is_str_date else ensure_pendulum_datetime_non_utc(value)
        )

    # we must have pendulum instance.
    if isinstance(value, date):
        # we didn't convert to pendulum yet
        if not value_format:
            value = (
                ensure_pendulum_datetime_non_utc(value)
                if isinstance(value, datetime)
                else ensure_pendulum_date(value)
            )
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
    value: pendulum.DateTime,
    last_value_func: LastValueFunc[TCursorValue],
) -> pendulum.DateTime:
    if last_value_func is max:
        lag = -lag

    if isinstance(value, pendulum.DateTime):
        return value.add(seconds=lag)

    return value.add(days=lag)


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
    return lagged_last_value  # type: ignore[no-any-return]
