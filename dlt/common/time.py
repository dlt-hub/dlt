from typing import Optional  # noqa

from dlt.common import signals

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


def sleep(sleep_seconds: float) -> None:
    """A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period."""
    # do not allow sleeping if signal was received
    signals.raise_if_signalled()
    # sleep or wait for signal
    signals.exit_event.wait(sleep_seconds)
    # if signal then raise
    signals.raise_if_signalled()