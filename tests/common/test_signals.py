import os
import pytest
import time
from multiprocessing.dummy import Process

from dlt.common import signals
from dlt.common.time import sleep
from dlt.common.exceptions import SignalReceivedException


@pytest.fixture(autouse=True)
def clear_signal() -> None:
    yield
    signals.exit_event.clear()
    signals._received_signal = 0


def test_sleep() -> None:
    start = time.time()
    sleep(0.5)
    assert time.time() - start - 0.5 < 0.01


def test_sleep_raises_if_signalled() -> None:
    signals.signal_receiver(4, None)
    with pytest.raises(SignalReceivedException) as exc:
        sleep(0.1)
    assert exc.value.signal_code == 4


def test_signal_receiver() -> None:
    signals.signal_receiver(8, None)
    assert signals._received_signal == 8
    # second signal gets ignored
    signals.signal_receiver(4, None)
    assert signals._received_signal == 8


def test_raise_if_signalled() -> None:
    signals.raise_if_signalled()
    signals.signal_receiver(8, None)
    with pytest.raises(SignalReceivedException) as exc:
        signals.raise_if_signalled()
    assert exc.value.signal_code == 8


def test_sleep_signal() -> None:

    thread_signal = 0

    def _thread() -> None:
        nonlocal thread_signal

        try:
            # this will sleep on exit event forever
            sleep(1000000)
        except SignalReceivedException as siex:
            thread_signal = siex.signal_code

    p = Process(target=_thread)
    p.start()
    time.sleep(0.1)
    # this sets exit event
    signals.signal_receiver(4, None)
    p.join()
    assert thread_signal == 4


@pytest.mark.forked
def test_signalling() -> None:
    signals.register_signals()

    thread_signal = 0

    def _thread() -> None:
        nonlocal thread_signal

        try:
            # this will sleep on exit event forever
            sleep(1000000)
        except SignalReceivedException as siex:
            thread_signal = siex.signal_code

    p = Process(target=_thread)
    p.start()

    # now signal to itself
    os.kill(os.getpid(), signals.signal.SIGTERM)
    # handler is executed in the main thread (here)
    with pytest.raises(SignalReceivedException) as exc:
        signals.raise_if_signalled()
    assert exc.value.signal_code == 15
    p.join()
    assert thread_signal == 15
