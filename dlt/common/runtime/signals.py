import os
import sys
import threading
import signal
from contextlib import contextmanager
from threading import Event
from types import FrameType
from typing import Any, Callable, Dict, Iterator, Optional, Union

from dlt.common.exceptions import SignalReceivedException

_received_signal: int = 0
exit_event = Event()
_signal_counts: Dict[int, int] = {}
_original_handlers: Dict[int, Union[int, Callable[[int, Optional[FrameType]], Any]]] = {}

# NOTE: do not use logger and print in signal handlers


def _signal_receiver(sig: int, frame: FrameType) -> None:
    """Handle POSIX signals with two-stage escalation.

    This handler is installed by intercepted_signals(). On the first occurrence of a
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
    # track how many times this signal type has been received
    _signal_counts[sig] = _signal_counts.get(sig, 0) + 1

    if _signal_counts[sig] == 1:
        # first signal of this type: set flag and wake threads
        set_received_signal(sig)
        if sys.stdin.isatty():
            # log to console using low level functions that are safe for signal handlers
            if sig == signal.SIGINT:
                sig_desc = "CTRL-C"
            else:
                sig_desc = f"Signal {sig}"
            msg = (
                f"{sig_desc} received. Trying to shut down gracefully. It may take time to drain"
                f" job pools. Send {sig_desc} again to force stop.\n"
            )
            try:
                os.write(sys.stderr.fileno(), msg.encode(encoding="utf-8"))
            except OSError:
                pass
    elif _signal_counts[sig] >= 2:
        # second signal of this type: call original handler
        original_handler = _original_handlers.get(sig, signal.SIG_DFL)
        if callable(original_handler):
            original_handler(sig, frame)
        elif original_handler == signal.SIG_DFL:
            # restore default and re-raise to trigger default behavior
            signal.signal(sig, signal.SIG_DFL)
            signal.raise_signal(sig)

    exit_event.set()


def _clear_signals() -> None:
    global _received_signal

    _received_signal = 0
    _signal_counts.clear()
    _original_handlers.clear()


def set_received_signal(sig: int) -> None:
    """Called when signal was received"""
    global _received_signal

    _received_signal = sig


def raise_if_signalled() -> None:
    """Raises `SignalReceivedException` if signal was received."""
    if was_signal_received():
        raise exception_for_signal()


def exception_for_signal() -> BaseException:
    if not was_signal_received():
        raise RuntimeError("no signal received")
    return SignalReceivedException(_received_signal)


def was_signal_received() -> bool:
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


@contextmanager
def intercepted_signals() -> Iterator[None]:
    """Will intercept SIGINT and SIGTERM and will delay calling signal handlers until
    `raise_if_signalled` is explicitly used or when a second signal with the same int value arrives.

    A no-op when not called on main thread.

    Can be nested - nested calls are no-ops.
    """

    if threading.current_thread() is threading.main_thread():
        # check if handlers are already installed (nested call)
        current_sigint_handler = signal.getsignal(signal.SIGINT)

        if current_sigint_handler is _signal_receiver:
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
            signal.signal(signal.SIGINT, _signal_receiver)
            signal.signal(signal.SIGTERM, _signal_receiver)
            yield
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)
            signal.signal(signal.SIGTERM, original_sigterm_handler)
            _clear_signals()

    else:
        from dlt.common import logger

        logger.info("Running in daemon thread, signals not enabled")
        yield
