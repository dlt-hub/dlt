from typing import Literal, Optional, TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration

TPoolType = Literal["process", "thread", "none"]


@configspec
class PoolRunnerConfiguration(BaseConfiguration):
    pool_type: TPoolType = None  # type of pool to run, must be set in derived configs
    workers: Optional[int] = None  # how many threads/processes in the pool
    run_sleep: float = 0.1  # how long to sleep between runs with workload, seconds

    if TYPE_CHECKING:
        def __init__(
            self,
            pool_type: TPoolType = None,
            workers: int = None
        ) -> None:
            ...
