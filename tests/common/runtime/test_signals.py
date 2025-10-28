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
    signals._clear_signals()


def test_sleep() -> None:
    start = time.time()
    sleep(0.5)
    # why delta is so big? (0.2) -> tests on mac require that
    assert time.time() - start - 0.5 < 0.2


def test_sleep_not_raises_if_signalled() -> None:
    signals._signal_receiver(4, None)
    sleep(0.1)


def test_signal_receiver() -> None:
    signals._signal_receiver(8, None)
    assert signals._received_signal == 8
    assert signals._signal_counts[8] == 1

    signals._signal_receiver(4, None)
    assert signals._received_signal == 4
    assert signals._signal_counts[4] == 1


def test_raise_if_signalled() -> None:
    signals.raise_if_signalled()
    signals._signal_receiver(8, None)
    with pytest.raises(SignalReceivedException) as exc:
        signals.raise_if_signalled()
    assert exc.value.signal_code == 8


def test_delayed_signals_context_manager() -> None:
    signals.raise_if_signalled()

    with signals.intercepted_signals():
        with pytest.raises(SignalReceivedException):
            signals._signal_receiver(2, None)
            # now it raises
            signals.raise_if_signalled()

    # and now it is disabled
    try:
        signals.raise_if_signalled()
    except SignalReceivedException:
        pytest.fail("Unexpected SignalReceivedException was raised")


def test_raise_if_signalled_thread() -> None:
    thread_signal = 0

    def _thread() -> None:
        nonlocal thread_signal

        try:
            # this will sleep on exit event forever
            sleep(100000)
            assert signals.was_signal_received()
            signals.raise_if_signalled()
        except SignalReceivedException as siex:
            thread_signal = siex.signal_code

    p = DummyProcess(target=_thread)
    p.start()
    time.sleep(0.1)
    # this sets exit event
    signals._signal_receiver(4, None)
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
def test_signalling_graceful() -> None:
    def _thread() -> None:
        # this will sleep on exit event forever
        sleep(1000000)

    p = DummyProcess(target=_thread)
    p.start()

    # handle signals without killing the process
    with signals.intercepted_signals():
        # now signal to itself
        os.kill(os.getpid(), signals.signal.SIGTERM)

        # handler is executed in the main thread (here)
        with pytest.raises(SignalReceivedException) as exc:
            signals.raise_if_signalled()
        assert exc.value.signal_code == 15
        p.join()


@skipifwindows
@pytest.mark.forked
def test_signalling_forced() -> None:
    _done = False

    def _thread() -> None:
        from time import sleep

        # this will block forever
        while not _done:
            sleep(1)

    p = DummyProcess(target=_thread)
    p.start()

    # handle signals without killing the process
    with signals.intercepted_signals():
        # now signal to itself
        os.kill(os.getpid(), signals.signal.SIGINT)

        # second signal kills immediately (SIGTERM would just exit process)
        with pytest.raises(KeyboardInterrupt):
            os.kill(os.getpid(), signals.signal.SIGINT)

        _done = True
        p.join()


def test_cleanup() -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass
