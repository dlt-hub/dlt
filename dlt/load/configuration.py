from typing import TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import LoadVolumeConfiguration, PoolRunnerConfiguration, TPoolType


@configspec(init=True)
class LoaderConfiguration(PoolRunnerConfiguration):
    workers: int = 20  # how many parallel loads can be executed
    pool_type: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool
    always_wipe_storage: bool = False  # removes all data in the storage
    _load_storage_config: LoadVolumeConfiguration = None

    if TYPE_CHECKING:
        def __init__(
            self,
            pool_type: TPoolType = None,
            workers: int = None,
            exit_on_exception: bool = None,
            is_single_run: bool = None,
            always_wipe_storage: bool = None,
            _load_storage_config: LoadVolumeConfiguration = None
        ) -> None:
            ...
