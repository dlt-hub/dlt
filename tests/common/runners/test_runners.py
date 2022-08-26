import pytest
import multiprocessing
from typing import Type
from prometheus_client import registry

from dlt.cli import TRunnerArgs
from dlt.common import signals
from dlt.common.typing import StrAny
from dlt.common.configuration import PoolRunnerConfiguration, make_configuration
from dlt.common.configuration.pool_runner_configuration import TPoolType
from dlt.common.exceptions import DltException, SignalReceivedException, TimeRangeExhaustedException, UnsupportedProcessStartMethodException
from dlt.common.runners import pool_runner as runner

from tests.common.runners.utils import _TestRunnable
from tests.utils import init_logger

class ModPoolRunnerConfiguration(PoolRunnerConfiguration):
    IS_SINGLE_RUN: bool = True
    WAIT_RUNS: int = 1
    PIPELINE_NAME: str = "testrunners"
    POOL_TYPE: TPoolType = "none"
    RUN_SLEEP: float = 0.1
    RUN_SLEEP_IDLE: float = 0.1
    RUN_SLEEP_WHEN_FAILED: float = 0.1


class StopExceptionRunnerConfiguration(ModPoolRunnerConfiguration):
    EXIT_ON_EXCEPTION: bool = True


class LimitedPoolRunnerConfiguration(ModPoolRunnerConfiguration):
    STOP_AFTER_RUNS: int = 5


class ProcessPoolConfiguration(ModPoolRunnerConfiguration):
    POOL_TYPE: TPoolType = "process"


class ThreadPoolConfiguration(ModPoolRunnerConfiguration):
    POOL_TYPE: TPoolType = "thread"


def configure(C: Type[PoolRunnerConfiguration], args: TRunnerArgs) -> Type[PoolRunnerConfiguration]:
    return make_configuration(C, C, initial_values=args._asdict())


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_logger(ModPoolRunnerConfiguration)


@pytest.fixture(autouse=True)
def default_args() -> None:
    signals._received_signal = 0
    runner.create_gauges(registry.CollectorRegistry(auto_describe=True))


# test runner functions
def idle_run(pool: None) -> runner.TRunMetrics:
    return runner.TRunMetrics(True, False, 0)


def non_idle_run(pool: None) -> runner.TRunMetrics:
    return runner.TRunMetrics(False, False, 0)


def short_workload_run(pool: None) -> runner.TRunMetrics:
    # 2 idle runs -> 2 pending runs -> 1 idle run - should be the last
    gauges = runner.update_gauges()
    if gauges["runs_count"] < 3 or gauges["runs_count"] > 4:
        return runner.TRunMetrics(True, False, 0)
    return  runner.TRunMetrics(False, False, 1)


def failing_run(pool: None) -> runner.TRunMetrics:
    raise DltException()


def good_then_failing_run(pool: None) -> runner.TRunMetrics:
    # 2 good runs, then failing
    gauges = runner.update_gauges()
    if gauges["runs_count"] < 3:
        return runner.TRunMetrics(False, False, 1)
    raise DltException()


def failing_then_good_run(pool: None) -> runner.TRunMetrics:
    # 2 good runs, then failing
    gauges = runner.update_gauges()
    if gauges["runs_count"] < 3:
        raise DltException()

    return runner.TRunMetrics(False, False, 1)


def signal_exception_run(pool: None) -> runner.TRunMetrics:
    signals._received_signal = 9
    raise SignalReceivedException(9)


def timerange_exhausted_run(pool: None) -> runner.TRunMetrics:
    raise TimeRangeExhaustedException(1575314188.1735284, 1575314288.8058035)


def signal_pending_run(pool: None) -> runner.TRunMetrics:
    signals._received_signal = 9
    # normal processing
    return runner.TRunMetrics(False, False, 1)


def test_single_idle_run() -> None:
    code = runner.run_pool(ModPoolRunnerConfiguration, idle_run)
    assert code == 0
    assert runner.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 1,
        "runs_cs_healthy_gauge": 1,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_single_failing_run() -> None:
    code = runner.run_pool(ModPoolRunnerConfiguration, failing_run)
    assert code == 0
    assert runner.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 0,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 1,
        "runs_cs_failed_gauge": 1,
        "runs_pending_items_gauge": -1
    }


def test_good_then_failing_run() -> None:
    # end after 5 runs
    code = runner.run_pool(
        configure(LimitedPoolRunnerConfiguration, TRunnerArgs(False, 0)),
        good_then_failing_run
    )
    assert code == -2
    assert runner.update_gauges() == {
        "runs_count": 5,
        "runs_not_idle_count": 2,
        "runs_healthy_count": 2,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 3,
        "runs_cs_failed_gauge": 3,
        "runs_pending_items_gauge": -1
    }


def test_failing_then_good_run() -> None:
    # end after 5 runs
    code = runner.run_pool(
        configure(LimitedPoolRunnerConfiguration, TRunnerArgs(False, 0)),
        failing_then_good_run
    )
    assert code == -2
    assert runner.update_gauges() == {
        "runs_count": 5,
        "runs_not_idle_count": 3,
        "runs_healthy_count": 3,
        "runs_cs_healthy_gauge": 3,
        "runs_failed_count": 2,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 1
    }


def test_stop_on_exception() -> None:
    # stop on exception will pass the exception to the run_pool host
    with pytest.raises(DltException):
        runner.run_pool(
            configure(StopExceptionRunnerConfiguration, TRunnerArgs(False, 0)),
            good_then_failing_run
        )
    # gauges must be updated in finally
    assert runner.update_gauges() == {
        "runs_count": 3,
        "runs_not_idle_count": 2,
        "runs_healthy_count": 2,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 1,
        "runs_cs_failed_gauge": 1,
        "runs_pending_items_gauge": -1
    }


def test_stop_on_signal_pending_run() -> None:
    code = runner.run_pool(
        configure(StopExceptionRunnerConfiguration, TRunnerArgs(False, 0)),
        signal_pending_run
    )
    assert code == 9
    assert runner.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 1,
        "runs_healthy_count": 1,
        "runs_cs_healthy_gauge": 1,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 1
    }


def test_stop_after_max_runs() -> None:
    # end after 5 runs
    code = runner.run_pool(
        configure(LimitedPoolRunnerConfiguration, TRunnerArgs(False, 0)),
        failing_then_good_run
    )
    assert code == -2
    assert runner.update_gauges()["runs_count"] == 5


def test_signal_exception_run() -> None:
    code = runner.run_pool(
        configure(ModPoolRunnerConfiguration, TRunnerArgs(False, 0)),
        signal_exception_run
    )
    assert code == 9
    assert runner.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 0,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_timerange_exhausted_run() -> None:
    code = runner.run_pool(
        configure(ModPoolRunnerConfiguration, TRunnerArgs(False, 0)),
        timerange_exhausted_run
    )
    assert code == 0
    assert runner.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 0,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_single_non_idle_run() -> None:
    code = runner.run_pool(ModPoolRunnerConfiguration, non_idle_run)
    assert code == 0
    assert runner.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 1,
        "runs_healthy_count": 1,
        "runs_cs_healthy_gauge": 1,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_single_run_short_wl() -> None:
    # so we get into pending but not past it
    code = runner.run_pool(
        configure(ModPoolRunnerConfiguration, TRunnerArgs(True, 3)),
        short_workload_run
    )
    assert code == 0
    assert runner.update_gauges() == {
        "runs_count": 5,
        "runs_not_idle_count": 2,
        "runs_healthy_count": 5,
        "runs_cs_healthy_gauge": 5,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_runnable_with_runner() -> None:
    r = _TestRunnable(4)
    code = runner.run_pool(
        configure(ThreadPoolConfiguration, TRunnerArgs(True, 0)),
        r
    )
    assert code == 0
    assert [v[0] for v in r.rv] == list(range(4))


@pytest.mark.forked
def test_spawn_pool() -> None:
    multiprocessing.set_start_method("spawn", force=True)
    with pytest.raises(UnsupportedProcessStartMethodException) as exc:
        runner.run_pool(ProcessPoolConfiguration, idle_run)
    assert exc.value.method == "spawn"
