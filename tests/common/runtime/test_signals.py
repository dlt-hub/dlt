import os
import pytest
import time
from multiprocessing.dummy import Process as DummyProcess
from typing import Iterator

from dlt.common import sleep
from dlt.common.exceptions import SignalReceivedException
from dlt.common.runtime import signals

from tests.utils import skipifwindows


@pytest.fixture(autouse=True)
def clear_signal() -> Iterator[None]:
    yield
    signals.exit_event.clear()
    signals._received_signal = 0


def test_sleep() -> None:
    start = time.time()
    sleep(0.5)
    # why delta is so big? (0.2) -> tests on mac require that
    assert time.time() - start - 0.5 < 0.2


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


def test_delayed_signals_context_manager() -> None:
    signals.raise_if_signalled()

    with signals.delayed_signals():
        with pytest.raises(SignalReceivedException):
            signals.signal_receiver(2, None)
            # now it raises
            signals.raise_if_signalled()

    # and now it is disabled
    try:
        signals.raise_if_signalled()
    except SignalReceivedException:
        pytest.fail("Unexpected SignalReceivedException was raised")


def test_sleep_signal() -> None:
    thread_signal = 0

    def _thread() -> None:
        nonlocal thread_signal

        try:
            # this will sleep on exit event forever
            sleep(1000000)
        except SignalReceivedException as siex:
            thread_signal = siex.signal_code

    p = DummyProcess(target=_thread)
    p.start()
    time.sleep(0.1)
    # this sets exit event
    signals.signal_receiver(4, None)
    p.join()
    assert thread_signal == 4


def test_raise_signal_received_exception() -> None:
    with pytest.raises(SignalReceivedException):
        # make sure Exception does not catch SignalReceivedException
        try:
            raise SignalReceivedException(2)
        except Exception:
            pass


@skipifwindows
@pytest.mark.forked
def test_signalling() -> None:
    thread_signal = 0

    def _thread() -> None:
        nonlocal thread_signal

        try:
            # this will sleep on exit event forever
            sleep(1000000)
        except SignalReceivedException as siex:
            thread_signal = siex.signal_code

    p = DummyProcess(target=_thread)
    p.start()

    # handle signals without killing the process
    with signals.delayed_signals():
        # now signal to itself
        os.kill(os.getpid(), signals.signal.SIGTERM)
        # handler is executed in the main thread (here)
        with pytest.raises(SignalReceivedException) as exc:
            signals.raise_if_signalled()
        assert exc.value.signal_code == 15
        p.join()
        assert thread_signal == 15
