import signal
from contextlib import contextmanager
from threading import Event
from typing import Any, TYPE_CHECKING, Iterator

from dlt.common.exceptions import SignalReceivedException

_received_signal: int = 0
_raise_immediately: bool = False
_original_sigint_handler = signal.getsignal(signal.SIGINT)
_original_sigterm_handler = signal.getsignal(signal.SIGTERM)
exit_event = Event()


def signal_receiver(sig: int, frame: Any) -> None:
    if not TYPE_CHECKING:
        from dlt.common.runtime import logger
    else:
        logger: Any = None

    global _received_signal

    logger.info(f"Signal {sig} received")

    if _received_signal > 0:
        logger.info(f"Another signal received after {_received_signal}")
        return

    _received_signal = sig
    # awake all threads sleeping on event
    exit_event.set()

    logger.info("Sleeping threads signalled")

    if _raise_immediately and _received_signal == signal.SIGINT:
        raise_if_signalled()


def raise_if_signalled() -> None:
    if _received_signal:
        raise SignalReceivedException(_received_signal)


def register_signals() -> None:
    signal.signal(signal.SIGINT, signal_receiver)
    signal.signal(signal.SIGTERM, signal_receiver)


def restore_signals() -> None:
    signal.signal(signal.SIGINT, _original_sigint_handler)
    signal.signal(signal.SIGTERM, _original_sigterm_handler)


def sleep(sleep_seconds: float) -> None:
    """A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period."""
    # do not allow sleeping if signal was received
    raise_if_signalled()
    # sleep or wait for signal
    exit_event.wait(sleep_seconds)
    # if signal then raise
    raise_if_signalled()


@contextmanager
def raise_immediately() -> Iterator[None]:
    global _raise_immediately

    try:
        _raise_immediately = True
        yield
    finally:
        _raise_immediately = False
