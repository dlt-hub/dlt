import pytest
import multiprocessing
from typing import Type

from dlt.common.runtime import signals
from dlt.common.configuration import resolve_configuration, configspec
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.exceptions import DltException, SignalReceivedException
from dlt.common.runners import pool_runner as runner
from dlt.common.runtime import apply_runtime_config
from dlt.common.runners.configuration import PoolRunnerConfiguration, TPoolType

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

    apply_runtime_config(config)

    assert logger.LOGGER is not None
    logger.warning("hello")


@pytest.mark.parametrize("method", ALL_METHODS)
def test_pool_runner_process_methods_forced(method) -> None:
    multiprocessing.set_start_method(method, force=True)
    r = _TestRunnableWorker(4)
    # make sure signals and logging is initialized
    C = resolve_configuration(RuntimeConfiguration())
    apply_runtime_config(C)

    runs_count = runner.run_pool(configure(ProcessPoolConfiguration), r)
    assert runs_count == 1
    assert [v[0] for v in r.rv] == list(range(4))


@pytest.mark.parametrize("method", ALL_METHODS)
def test_pool_runner_process_methods_configured(method) -> None:
    r = _TestRunnableWorker(4)
    # make sure signals and logging is initialized
    C = resolve_configuration(RuntimeConfiguration())
    apply_runtime_config(C)

    runs_count = runner.run_pool(ProcessPoolConfiguration(start_method=method), r)
    assert runs_count == 1
    assert [v[0] for v in r.rv] == list(range(4))
