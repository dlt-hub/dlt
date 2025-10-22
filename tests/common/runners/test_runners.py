import pytest
import sys
import time
import multiprocessing
from typing import Type

from dlt.common.runtime import signals
from dlt.common.configuration import resolve_configuration, configspec
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.exceptions import DltException, SignalReceivedException
from dlt.common.runners import pool_runner as runner
from dlt.common.runners.configuration import PoolRunnerConfiguration, TPoolType

from dlt.common.runtime.init import initialize_runtime
from tests.common.runners.utils import (
    _TestRunnableWorkerMethod,
    _TestRunnableWorker,
    ALL_METHODS,
    mp_method_auto,
)
from tests.utils import init_test_logging


@configspec
class ModPoolRunnerConfiguration(PoolRunnerConfiguration):
    pipeline_name: str = "testrunners"
    pool_type: TPoolType = "none"
    run_sleep: float = 0.1


@configspec
class ProcessPoolConfiguration(ModPoolRunnerConfiguration):
    pool_type: TPoolType = "process"


@configspec
class ThreadPoolConfiguration(ModPoolRunnerConfiguration):
    pool_type: TPoolType = "thread"


def configure(C: Type[PoolRunnerConfiguration]) -> PoolRunnerConfiguration:
    default = C()
    return resolve_configuration(default)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


_counter = 0


@pytest.fixture(autouse=True)
def default_args() -> None:
    signals._received_signal = 0
    global _counter
    _counter = 0


# test runner functions
def idle_run(pool: None) -> runner.TRunMetrics:
    return runner.TRunMetrics(True, 0)


def non_idle_run(pool: None) -> runner.TRunMetrics:
    return runner.TRunMetrics(False, 0)


def failing_run(pool: None) -> runner.TRunMetrics:
    raise DltException()


def good_then_failing_run(pool: None) -> runner.TRunMetrics:
    # 2 good runs, then failing
    global _counter
    _counter += 1
    if _counter < 3:
        return runner.TRunMetrics(False, 1)
    raise DltException()


def signal_exception_run(pool: None) -> runner.TRunMetrics:
    signals._received_signal = 9
    raise SignalReceivedException(9)


def signal_pending_run(pool: None) -> runner.TRunMetrics:
    signals._received_signal = 9
    # normal processing
    return runner.TRunMetrics(False, 1)


def test_single_idle_run() -> None:
    runs_count = runner.run_pool(ModPoolRunnerConfiguration(), idle_run)
    assert runs_count == 1


def test_single_failing_run() -> None:
    with pytest.raises(DltException):
        runner.run_pool(ModPoolRunnerConfiguration(), failing_run)


def test_good_then_failing_run() -> None:
    # end on 3rd run
    with pytest.raises(DltException):
        runner.run_pool(ModPoolRunnerConfiguration(), good_then_failing_run)
    assert _counter == 3


def test_stop_on_signal_pending_run() -> None:
    with pytest.raises(SignalReceivedException):
        runner.run_pool(ModPoolRunnerConfiguration(), signal_pending_run)


def test_signal_exception_run() -> None:
    with pytest.raises(SignalReceivedException):
        runner.run_pool(ModPoolRunnerConfiguration(), signal_exception_run)


def test_single_non_idle_run() -> None:
    runs_count = runner.run_pool(ModPoolRunnerConfiguration(), non_idle_run)
    assert runs_count == 1


def test_runnable_with_runner() -> None:
    r = _TestRunnableWorkerMethod(4)
    runs_count = runner.run_pool(configure(ThreadPoolConfiguration), r)
    assert runs_count == 1
    assert [v[0] for v in r.rv] == list(range(4))


@pytest.mark.forked
def test_initialize_runtime() -> None:
    config = resolve_configuration(RuntimeConfiguration())
    config.log_level = "INFO"

    from dlt.common import logger

    logger._delete_current_logger()
    logger.LOGGER = None

    initialize_runtime("dlt", config)

    assert logger.LOGGER is not None
    logger.warning("hello")


@pytest.mark.parametrize("method", ALL_METHODS)
def test_pool_runner_process_methods_forced(method) -> None:
    multiprocessing.set_start_method(method, force=True)
    r = _TestRunnableWorker(4)
    # make sure signals and logging is initialized
    config = resolve_configuration(RuntimeConfiguration())
    initialize_runtime("dlt", config)

    runs_count = runner.run_pool(configure(ProcessPoolConfiguration), r)
    assert runs_count == 1
    assert [v[0] for v in r.rv] == list(range(4))


@pytest.mark.parametrize("method", ALL_METHODS)
def test_pool_runner_process_methods_configured(method) -> None:
    r = _TestRunnableWorker(4)
    # make sure signals and logging is initialized
    config = resolve_configuration(RuntimeConfiguration())
    initialize_runtime("dlt", config)

    runs_count = runner.run_pool(ProcessPoolConfiguration(start_method=method), r)
    assert runs_count == 1
    assert [v[0] for v in r.rv] == list(range(4))


import threading

_tls = threading.local()


def lock_del_task():
    """Returns promptly, but its local object's __del__ blocks for 2 s."""

    class Blocker:
        def __del__(self):
            time.sleep(2)

    # store blocker in local thread storage so it is garbage collected on thread exit
    _tls.blocker = Blocker()
    return "OK"


def test_pool_runner_shutdown_timeout() -> None:
    pool = runner.TimeoutThreadPoolExecutor(max_workers=4, timeout=1.1)

    t0 = time.perf_counter()

    assert pool.submit(lock_del_task).result() == "OK"
    # was not waiting in submit
    assert time.perf_counter() - t0 < 0.3
    # assert that threads were alive
    pool.shutdown(wait=True)
    assert pool._is_alive is True
    # and was waiting 1 second, not 2
    assert time.perf_counter() - t0 > 1.0
    assert time.perf_counter() - t0 < 2.0

    # now wait again this time should not be alive
    pool.shutdown(wait=True)
    assert pool._is_alive is False


def test_use_null_executor_on_non_threading_platform(monkeypatch) -> None:
    # regular platform
    config = resolve_configuration(ModPoolRunnerConfiguration())
    config.pool_type = "process"
    pool = runner.create_pool(config)
    assert not isinstance(pool, runner.NullExecutor)
    config.pool_type = "thread"
    pool = runner.create_pool(config)
    assert not isinstance(pool, runner.NullExecutor)
    config.pool_type = None
    pool = runner.create_pool(config)
    assert isinstance(pool, runner.NullExecutor)

    # non-threading platform
    monkeypatch.setattr(sys, "platform", "emscripten")
    config = resolve_configuration(ModPoolRunnerConfiguration())
    config.pool_type = "process"
    pool = runner.create_pool(config)
    assert isinstance(pool, runner.NullExecutor)
    config.pool_type = "thread"
    pool = runner.create_pool(config)
    assert isinstance(pool, runner.NullExecutor)
    config.pool_type = None
    pool = runner.create_pool(config)
    assert isinstance(pool, runner.NullExecutor)
