from typing import Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, PoolRunnerConfiguration, CredentialsConfiguration, TPoolType
from dlt.common.configuration.specs.load_volume_configuration import LoadVolumeConfiguration


@configspec(init=True)
class DestinationClientConfiguration(BaseConfiguration):
    destination_name: str = None  # which destination to load data to
    credentials: Optional[CredentialsConfiguration]


@configspec(init=True)
class DestinationClientDwhConfiguration(DestinationClientConfiguration):
    dataset_name: str = None  # dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
    default_schema_name: Optional[str] = None  # name of default schema to be used to name effective dataset to load data to


@configspec(init=True)
class LoaderConfiguration(PoolRunnerConfiguration):
    workers: int = 20  # how many parallel loads can be executed
    pool_type: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool
    load_storage_config: LoadVolumeConfiguration = None
