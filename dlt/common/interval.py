from datetime import datetime, timedelta  # noqa: I251

from croniter import croniter

from dlt.common.time import parse_period_seconds
from dlt.common.typing import TTimeInterval


def is_cron_expression(s: str) -> bool:
    """Check whether a string is a valid cron expression.

    Args:
        s (str): The string to validate.

    Returns:
        bool: `True` if `s` parses as a cron expression.
    """
    return croniter.is_valid(s)


def lag_cron(cron_expr: str, dt: datetime, count: int) -> datetime:
    """Floor `dt` to the latest cron tick, then step `count` ticks into the past.

    Ticks are computed on the naive wall clock of `dt`'s timezone, so daily and
    sub-daily expressions align correctly across DST transitions.

    Args:
        cron_expr (str): A cron expression defining the tick grid.
        dt (datetime): The reference time. Its timezone is preserved on the result.
        count (int): Number of ticks to step back. `count=0` returns the floor tick
            (`dt` itself when it falls on a tick); negative values step into the future
            (`count=-1` is the first tick strictly after the floor).

    Returns:
        datetime: The resulting tick, carrying `dt`'s timezone.
    """
    # get_prev returns the tick strictly before its base, so step forward by one
    # microsecond to include dt itself when it falls exactly on a tick
    base = (dt.replace(tzinfo=None) if dt.tzinfo else dt) + timedelta(microseconds=1)
    cron = croniter(cron_expr, base)
    tick: datetime = cron.get_prev(datetime)
    for _ in range(abs(count)):
        tick = cron.get_prev(datetime) if count > 0 else cron.get_next(datetime)
    if dt.tzinfo is not None:
        return tick.replace(tzinfo=dt.tzinfo)
    return tick


def lag_interval(
    interval: TTimeInterval,
    trigger: str,
    count: int = 1,
    lag_end: bool = False,
) -> TTimeInterval:
    """Lag the interval start (or end) by `count` trigger ticks into the past.

    `count=0` snaps the bound to the tick floor, negative `count` moves it into the
    future. For `every:` triggers the bound shifts by `period * count` seconds.

    Args:
        interval (TTimeInterval): The interval to adjust.
        trigger (str): A `schedule:` or `every:` trigger, or a bare cron expression.
        count (int): Number of ticks (or periods) to lag; negative moves into the future.
            Defaults to 1.
        lag_end (bool): When `True`, adjusts the end instead of the start. Defaults to `False`.

    Returns:
        TTimeInterval: The adjusted interval.

    Raises:
        ValueError: If the adjusted interval is empty or negative, or if the trigger
            carries no time period (not a `schedule:`/`every:` trigger or cron expression).
    """
    s = str(trigger).strip()
    bound = interval.end if lag_end else interval.start
    if s.startswith("schedule:"):
        bound = lag_cron(s[len("schedule:") :], bound, count)
    elif s.startswith("every:"):
        period = parse_period_seconds(s[len("every:") :])
        bound = bound - timedelta(seconds=period * count)
    elif is_cron_expression(s):
        bound = lag_cron(s, bound, count)
    else:
        raise ValueError(
            f"trigger {s!r} has no time period, use a `schedule:`/`every:` trigger"
            " or a bare cron expression"
        )
    start, end = (interval.start, bound) if lag_end else (bound, interval.end)
    if start >= end:
        raise ValueError(f"interval [{start}, {end}) is empty or negative after lag")
    return TTimeInterval(start, end)


def full_days_interval(interval: TTimeInterval) -> TTimeInterval:
    """Widen an interval so it covers whole days.

    The start is floored to the midnight at or before it and the end raised to the
    midnight at or after it, each adjusted in its own timezone.

    Args:
        interval (TTimeInterval): The interval to widen.

    Returns:
        TTimeInterval: The widened interval.
    """
    daily = "0 0 * * *"
    # an end already at midnight covers whole days, so it stays put and widening is idempotent
    end_floor = lag_cron(daily, interval.end, 0)
    end = end_floor if end_floor == interval.end else lag_cron(daily, interval.end, -1)
    return TTimeInterval(lag_cron(daily, interval.start, 0), end)
