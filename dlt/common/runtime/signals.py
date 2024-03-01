import threading
import signal
from contextlib import contextmanager
from threading import Event
from typing import Any, TYPE_CHECKING, Iterator

from dlt.common.exceptions import SignalReceivedException

_received_signal: int = 0
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


def raise_if_signalled() -> None:
    if _received_signal:
        raise SignalReceivedException(_received_signal)


def sleep(sleep_seconds: float) -> None:
    """A signal-aware version of sleep function. Will raise SignalReceivedException if signal was received during sleep period."""
    # do not allow sleeping if signal was received
    raise_if_signalled()
    # sleep or wait for signal
    exit_event.wait(sleep_seconds)
    # if signal then raise
    raise_if_signalled()


@contextmanager
def delayed_signals() -> Iterator[None]:
    """Will delay signalling until `raise_if_signalled` is used or signalled `sleep`"""

    if threading.current_thread() is threading.main_thread():
        original_sigint_handler = signal.getsignal(signal.SIGINT)
        original_sigterm_handler = signal.getsignal(signal.SIGTERM)
        try:
            signal.signal(signal.SIGINT, signal_receiver)
            signal.signal(signal.SIGTERM, signal_receiver)
            yield
        finally:
            global _received_signal

            _received_signal = 0
            signal.signal(signal.SIGINT, original_sigint_handler)
            signal.signal(signal.SIGTERM, original_sigterm_handler)
    else:
        if not TYPE_CHECKING:
            from dlt.common.runtime import logger
        else:
            logger: Any = None
        logger.info("Running in daemon thread, signals not enabled")
        yield
