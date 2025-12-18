import os
import pytest
import sys
import time
import multiprocessing
from typing import ClassVar, Tuple, Type

from dlt.common.runtime import signals
from dlt.common.configuration import resolve_configuration, configspec
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import ConfigSectionContext, RuntimeConfiguration
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
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


@configspec
class SectionedTestConfig(BaseConfiguration):
    """A test configuration that uses a specific section."""

    test_value: str = "default"

    __section__: ClassVar[str] = "test_section"


def _worker_resolve_config() -> Tuple[str, Tuple[str, ...]]:
    """Worker function that resolves a config value using ConfigSectionContext.

    Returns:
        Tuple of (resolved_value, sections_from_context)
    """
    section_ctx = Container()[ConfigSectionContext]
    config = resolve_configuration(SectionedTestConfig())

    return config.test_value, section_ctx.sections


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


@pytest.mark.parametrize(
    "start_method",
    [
        "spawn",
        pytest.param(
            "fork",
            marks=pytest.mark.skipif(
                "fork" not in multiprocessing.get_all_start_methods(),
                reason="fork start method not available on this platform",
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "use_section_context",
    [True, False],
    ids=lambda x: "with_section_context" if x else "without_section_context",
)
def test_config_section_context_restored_in_worker(
    start_method: str, use_section_context: bool
) -> None:
    """Test that ConfigSectionContext is properly restored in worker processes.

    This test verifies that ConfigSectionContext is correctly serialized and restored
    in worker processes, allowing config resolution to use the correct sections.
    When no ConfigSectionContext is set, workers should use the default empty sections.
    """
    # Set up environment variables with section-specific values
    os.environ["MY_SECTION__TEST_SECTION__TEST_VALUE"] = "sectioned_value"
    os.environ["TEST_SECTION__TEST_VALUE"] = "non_sectioned_value"

    container = Container()

    if use_section_context:
        # Set up ConfigSectionContext in main process
        section_context = ConfigSectionContext(
            pipeline_name=None,
            sections=("my_section",),
        )
        container[ConfigSectionContext] = section_context
    elif ConfigSectionContext in container:
        # Ensure no ConfigSectionContext is in container
        del container[ConfigSectionContext]

    # Create process pool with multiple workers
    # Using multiple workers ensures we're actually testing cross-process behavior
    config = PoolRunnerConfiguration(
        pool_type="process",
        workers=4,
        start_method=start_method,
    )

    with runner.create_pool(config) as pool:
        # Submit multiple tasks to ensure we're using worker processes
        futures = [pool.submit(_worker_resolve_config) for _ in range(4)]
        results = [f.result() for f in futures]

        # All workers should have the same ConfigSectionContext
        result_value, result_sections = results[0]

    if use_section_context:
        # Verify that ConfigSectionContext was restored correctly
        assert result_sections == ("my_section",), (
            f"Expected sections ('my_section',) but got {result_sections}. "
            "ConfigSectionContext was not properly restored in worker process."
        )
        # Verify that config resolution used the correct sections
        assert result_value == "sectioned_value", (
            f"Expected 'sectioned_value' but got '{result_value}'. "
            "Config resolution did not use the restored ConfigSectionContext sections."
        )
    else:
        # Without section context, should use default empty sections
        assert result_sections == (), (
            f"Expected empty sections () but got {result_sections}. "
            "ConfigSectionContext should have default empty sections when not set."
        )
        # Verify that config resolution used the non-sectioned value
        assert result_value == "non_sectioned_value", (
            f"Expected 'non_sectioned_value' but got '{result_value}'. "
            "Config resolution should use non-sectioned value when no ConfigSectionContext is set."
        )
