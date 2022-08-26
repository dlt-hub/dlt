from typing import Literal, Optional
from dlt.common.configuration import RunConfiguration

TPoolType = Literal["process", "thread", "none"]


class PoolRunnerConfiguration(RunConfiguration):
    POOL_TYPE: TPoolType = None  # type of pool to run, must be set in derived configs
    WORKERS: Optional[int] = None  # how many threads/processes in the pool
    RUN_SLEEP: float = 0.5  # how long to sleep between runs with workload, seconds
    RUN_SLEEP_IDLE: float = 1.0  # how long to sleep when no more items are pending, seconds
    RUN_SLEEP_WHEN_FAILED: float = 1.0  # how long to sleep between the runs when failed
    IS_SINGLE_RUN: bool = False  # should run only once until all pending data is processed, and exit
    WAIT_RUNS: int = 0  # how many runs to wait for first data coming in is IS_SINGLE_RUN is set
    EXIT_ON_EXCEPTION: bool = False  # should exit on exception
    STOP_AFTER_RUNS: int = 10000  # will stop runner with exit code -2 after so many runs, that prevents memory fragmentation
