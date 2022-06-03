import pytest
import multiprocessing
from prometheus_client import registry

from dlt.common.exceptions import DltException, SignalReceivedException, TimeRangeExhaustedException, UnsupportedProcessStartMethodException
from dlt.common.configuration import PoolRunnerConfiguration
from dlt.common import runners, signals

from tests.utils import init_logger

class ModPoolRunnerConfiguration(PoolRunnerConfiguration):
    NAME: str = "testrunners"
    POOL_TYPE = "none"
    RUN_SLEEP: float = 0.1
    RUN_SLEEP_IDLE: float = 0.1
    RUN_SLEEP_WHEN_FAILED: float = 0.1


class StopExceptionRunnerConfiguration(ModPoolRunnerConfiguration):
    EXIT_ON_EXCEPTION: bool = True


class LimitedPoolRunnerConfiguration(ModPoolRunnerConfiguration):
    STOP_AFTER_RUNS: int = 5


class ProcessPolConfiguration(ModPoolRunnerConfiguration):
    POOL_TYPE = "process"


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_logger(ModPoolRunnerConfiguration)


@pytest.fixture(autouse=True)
def default_args() -> None:
    signals._received_signal = 0
    runners.RUN_ARGS = runners.TRunArgs(True, 1)
    runners.create_gauges(registry.CollectorRegistry(auto_describe=True))


# test runner functions
def idle_run(pool: None) -> runners.TRunMetrics:
    return runners.TRunMetrics(True, False, 0)


def non_idle_run(pool: None) -> runners.TRunMetrics:
    return runners.TRunMetrics(False, False, 0)


def short_workload_run(pool: None) -> runners.TRunMetrics:
    # 2 idle runs -> 2 pending runs -> 1 idle run - should be the last
    gauges = runners.update_gauges()
    if gauges["runs_count"] < 3 or gauges["runs_count"] > 4:
        return runners.TRunMetrics(True, False, 0)
    return  runners.TRunMetrics(False, False, 1)


def failing_run(pool: None) -> runners.TRunMetrics:
    raise DltException()


def good_then_failing_run(pool: None) -> runners.TRunMetrics:
    # 2 good runs, then failing
    gauges = runners.update_gauges()
    if gauges["runs_count"] < 3:
        return runners.TRunMetrics(False, False, 1)
    raise DltException()


def failing_then_good_run(pool: None) -> runners.TRunMetrics:
    # 2 good runs, then failing
    gauges = runners.update_gauges()
    if gauges["runs_count"] < 3:
        raise DltException()

    return runners.TRunMetrics(False, False, 1)


def signal_exception_run(pool: None) -> runners.TRunMetrics:
    signals._received_signal = 9
    raise SignalReceivedException(9)


def timerange_exhausted_run(pool: None) -> runners.TRunMetrics:
    raise TimeRangeExhaustedException(1575314188.1735284, 1575314288.8058035)


def signal_pending_run(pool: None) -> runners.TRunMetrics:
    signals._received_signal = 9
    # normal processing
    return runners.TRunMetrics(False, False, 1)


def test_single_idle_run() -> None:
    code = runners.pool_runner(ModPoolRunnerConfiguration, idle_run)
    assert code == 0
    assert runners.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 1,
        "runs_cs_healthy_gauge": 1,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_single_failing_run() -> None:
    code = runners.pool_runner(ModPoolRunnerConfiguration, failing_run)
    assert code == 0
    assert runners.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 0,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 1,
        "runs_cs_failed_gauge": 1,
        "runs_pending_items_gauge": -1
    }


def test_good_then_failing_run() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    # end after 5 runs
    code = runners.pool_runner(LimitedPoolRunnerConfiguration, good_then_failing_run)
    assert code == -2
    assert runners.update_gauges() == {
        "runs_count": 5,
        "runs_not_idle_count": 2,
        "runs_healthy_count": 2,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 3,
        "runs_cs_failed_gauge": 3,
        "runs_pending_items_gauge": -1
    }


def test_failing_then_good_run() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    # end after 5 runs
    code = runners.pool_runner(LimitedPoolRunnerConfiguration, failing_then_good_run)
    assert code == -2
    assert runners.update_gauges() == {
        "runs_count": 5,
        "runs_not_idle_count": 3,
        "runs_healthy_count": 3,
        "runs_cs_healthy_gauge": 3,
        "runs_failed_count": 2,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 1
    }


def test_stop_on_exception() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    code = runners.pool_runner(StopExceptionRunnerConfiguration, good_then_failing_run)
    assert code == -1
    assert runners.update_gauges() == {
        "runs_count": 3,
        "runs_not_idle_count": 2,
        "runs_healthy_count": 2,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 1,
        "runs_cs_failed_gauge": 1,
        "runs_pending_items_gauge": -1
    }


def test_stop_on_signal_pending_run() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    code = runners.pool_runner(StopExceptionRunnerConfiguration, signal_pending_run)
    assert code == 9
    assert runners.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 1,
        "runs_healthy_count": 1,
        "runs_cs_healthy_gauge": 1,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 1
    }


def test_stop_after_max_runs() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    # end after 5 runs
    code = runners.pool_runner(LimitedPoolRunnerConfiguration, failing_then_good_run)
    assert code == -2
    assert runners.update_gauges()["runs_count"] == 5


def test_signal_exception_run() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    code = runners.pool_runner(ModPoolRunnerConfiguration, signal_exception_run)
    assert code == 9
    assert runners.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 0,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_timerange_exhausted_run() -> None:
    runners.RUN_ARGS = runners.TRunArgs(False, 0)
    code = runners.pool_runner(ModPoolRunnerConfiguration, timerange_exhausted_run)
    assert code == 0
    assert runners.update_gauges() == {
        "runs_count": 1,
        "runs_not_idle_count": 0,
        "runs_healthy_count": 0,
        "runs_cs_healthy_gauge": 0,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


def test_single_non_idle_run() -> None:
    code = runners.pool_runner(ModPoolRunnerConfiguration, non_idle_run)
    assert code == 0
    assert runners.update_gauges() == {
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
    runners.RUN_ARGS = runners.TRunArgs(True, 3)
    code = runners.pool_runner(ModPoolRunnerConfiguration, short_workload_run)
    assert code == 0
    assert runners.update_gauges() == {
        "runs_count": 5,
        "runs_not_idle_count": 2,
        "runs_healthy_count": 5,
        "runs_cs_healthy_gauge": 5,
        "runs_failed_count": 0,
        "runs_cs_failed_gauge": 0,
        "runs_pending_items_gauge": 0
    }


@pytest.mark.forked
def test_spawn_pool() -> None:
    multiprocessing.set_start_method("spawn", force=True)
    with pytest.raises(UnsupportedProcessStartMethodException) as exc:
        runners.pool_runner(ProcessPolConfiguration, idle_run)
    assert exc.value.method == "spawn"
