import signal
from threading import Event
from typing import Any, Callable, TYPE_CHECKING

if not TYPE_CHECKING:
    from dlt.common import logger
else:
    logger: Any = None
from dlt.common.exceptions import SignalReceivedException

_received_signal: int = 0
exit_event = Event()


def signal_receiver(sig: int, frame: Any) -> None:
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


def register_signals() -> None:
    signal.signal(signal.SIGINT, signal_receiver)
    signal.signal(signal.SIGTERM, signal_receiver)
