from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.destination.capabilities import TLoaderParallelismStrategy
from dlt.common.storages import LoadStorageConfiguration
from dlt.common.runners.configuration import PoolRunnerConfiguration, TPoolType


@configspec
class LoaderConfiguration(PoolRunnerConfiguration):
    workers: int = 20
    """how many parallel loads can be executed"""
    parallelism_strategy: Optional[TLoaderParallelismStrategy] = None
    """Which parallelism strategy to use at load time"""
    pool_type: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool
    raise_on_failed_jobs: bool = False
    """when True, raises on terminally failed jobs immediately"""
    raise_on_max_retries: int = 5
    """When gt 0 will raise when job reaches raise_on_max_retries"""
    _load_storage_config: LoadStorageConfiguration = None
    # if set to `True`, the staging dataset will be
    # truncated after loading the data
    truncate_staging_dataset: bool = False

    def on_resolved(self) -> None:
        self.pool_type = (
            "none" if (self.workers == 1 or self.parallelism_strategy == "sequential") else "thread"
        )
