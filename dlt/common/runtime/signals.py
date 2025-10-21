import sys
import threading
import signal
from contextlib import contextmanager
from threading import Event
from types import FrameType
from typing import Any, Callable, Dict, Iterator, Optional, Union

from dlt.common import logger
from dlt.common.exceptions import SignalReceivedException

_received_signal: int = 0
exit_event = Event()
_signal_counts: Dict[int, int] = {}
_original_handlers: Dict[int, Union[int, Callable[[int, Optional[FrameType]], Any]]] = {}


def signal_receiver(sig: int, frame: FrameType) -> None:
    """Handle POSIX signals with two-stage escalation.

    This handler is installed by delayed_signals(). On the first occurrence of a
    supported signal (eg. SIGINT, SIGTERM) it requests a graceful shutdown by
    setting a process-wide flag and waking sleeping threads via exit_event.
    A second occurrence of the same signal escalates by delegating to the
    original handler or the system default, which typically results in an
    immediate process termination (eg. KeyboardInterrupt for SIGINT).

    Args:
        sig: Signal number (for example, signal.SIGINT or signal.SIGTERM).
        frame: The current stack frame when the signal was received.

    Notes:
        - The CPython runtime delivers signal handlers in the main thread only.
          Worker threads must cooperatively observe shutdown via raise_if_signalled()
          or the signal-aware sleep().
    """
    global _received_signal

    # track how many times this signal type has been received
    _signal_counts[sig] = _signal_counts.get(sig, 0) + 1

    if _signal_counts[sig] == 1:
        # first signal of this type: set flag and wake threads
        _received_signal = sig
        if sig == signal.SIGINT:
            sig_desc = "CTRL-C"
        else:
            sig_desc = f"Signal {sig}"
        msg = (
            f"{sig_desc} received. Trying to shut down gracefully. It may take time to drain job"
            f" pools. Send {sig_desc} again to force stop."
        )
        if sys.stdin.isatty():
            # log to console
            sys.stderr.write(msg)
            sys.stderr.flush()
        else:
            logger.warning(msg)
    elif _signal_counts[sig] >= 2:
        # Second signal of this type: call original handler
        logger.debug(f"Second signal {sig} received, calling default handler")
        original_handler = _original_handlers.get(sig, signal.SIG_DFL)
        if callable(original_handler):
            original_handler(sig, frame)
        elif original_handler == signal.SIG_DFL:
            # Restore default and re-raise to trigger default behavior
            signal.signal(sig, signal.SIG_DFL)
            signal.raise_signal(sig)

    exit_event.set()
    logger.debug("Sleeping threads signalled")


def raise_if_signalled() -> None:
    if _received_signal:
        raise SignalReceivedException(_received_signal)


def signal_received() -> bool:
    """check if a signal was received"""
    return True if _received_signal else False


def sleep(sleep_seconds: float) -> None:
    """A signal-aware version of sleep function. Will wake up if signal is received but will not raise exception."""
    # sleep or wait for signal
    exit_event.clear()
    exit_event.wait(sleep_seconds)


def wake_all() -> None:
    """Wakes all threads sleeping on event"""
    exit_event.set()


def _clear_signals() -> None:
    global _received_signal

    _received_signal = 0
    _signal_counts.clear()
    _original_handlers.clear()


@contextmanager
def delayed_signals() -> Iterator[None]:
    """Will delay signalling until `raise_if_signalled` is used or signalled `sleep`

    Can be nested - nested calls are no-ops.
    """

    if threading.current_thread() is threading.main_thread():
        # check if handlers are already installed (nested call)
        current_sigint_handler = signal.getsignal(signal.SIGINT)

        if current_sigint_handler is signal_receiver:
            # already installed, this is a nested call - just yield
            yield
            return

        # First call - install handlers
        original_sigint_handler = current_sigint_handler
        original_sigterm_handler = signal.getsignal(signal.SIGTERM)

        # store original handlers for signal_receiver to use
        _original_handlers[signal.SIGINT] = original_sigint_handler
        _original_handlers[signal.SIGTERM] = original_sigterm_handler

        try:
            signal.signal(signal.SIGINT, signal_receiver)
            signal.signal(signal.SIGTERM, signal_receiver)
            yield
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)
            signal.signal(signal.SIGTERM, original_sigterm_handler)
            _clear_signals()

    else:
        logger.info("Running in daemon thread, signals not enabled")
        yield
