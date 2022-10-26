from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, PoolRunnerConfiguration, CredentialsConfiguration, TPoolType
from dlt.common.configuration.specs.load_volume_configuration import LoadVolumeConfiguration


@configspec(init=True)
class LoaderConfiguration(PoolRunnerConfiguration):
    workers: int = 20  # how many parallel loads can be executed
    pool_type: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool
    always_wipe_storage: bool = False  # removes all data in the storage
    load_storage_config: LoadVolumeConfiguration = None
