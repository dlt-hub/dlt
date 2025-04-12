from typing import Literal, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration

TPoolType = Literal["process", "thread", "none"]


@configspec
class PoolRunnerConfiguration(BaseConfiguration):
    pool_type: TPoolType = None
    """type of pool to run, must be set in derived configs"""
    start_method: Optional[str] = None
    """start method for the pool (typically process). None is system default"""
    workers: Optional[int] = None
    """# how many threads/processes in the pool"""
    run_sleep: float = 0.1
    """how long to sleep between runs with workload, seconds"""
