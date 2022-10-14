from typing import Optional

from dlt.common.typing import StrAny
from dlt.common.configuration import configspec, make_configuration
from dlt.common.configuration.specs import BaseConfiguration, PoolRunnerConfiguration, LoadVolumeConfiguration, TPoolType

@configspec
class LoaderClientConfiguration(BaseConfiguration):
    client_type: str = None  # which destination to load data to


@configspec
class LoaderClientDwhConfiguration(LoaderClientConfiguration):
    default_dataset: str = None  # dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
    default_schema_name: Optional[str] = None  # name of default schema to be used to name effective dataset to load data to


@configspec
class LoaderConfiguration(PoolRunnerConfiguration, LoadVolumeConfiguration, LoaderClientConfiguration):
    workers: int = 20  # how many parallel loads can be executed
    pool_type: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool


def configuration(initial_values: StrAny = None) -> LoaderConfiguration:
    return make_configuration(LoaderConfiguration(), initial_value=initial_values)
