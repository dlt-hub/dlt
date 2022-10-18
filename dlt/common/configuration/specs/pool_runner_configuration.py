from typing import Literal, Optional

from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec

TPoolType = Literal["process", "thread", "none"]


@configspec
class PoolRunnerConfiguration(BaseConfiguration):
    pool_type: TPoolType = None  # type of pool to run, must be set in derived configs
    workers: Optional[int] = None  # how many threads/processes in the pool
    run_sleep: float = 0.5  # how long to sleep between runs with workload, seconds
    run_sleep_idle: float = 1.0  # how long to sleep when no more items are pending, seconds
    run_sleep_when_failed: float = 1.0  # how long to sleep between the runs when failed
    is_single_run: bool = False  # should run only once until all pending data is processed, and exit
    wait_runs: int = 0  # how many runs to wait for first data coming in is IS_SINGLE_RUN is set
    exit_on_exception: bool = False  # should exit on exception
    stop_after_runs: int = 10000  # will stop runner with exit code -2 after so many runs, that prevents memory fragmentation
