from typing import Any, Optional, Type
from dlt.common.configuration.run_configuration import BaseConfiguration

from dlt.common.typing import StrAny
from dlt.common.configuration import (PoolRunnerConfiguration,
                                              LoadVolumeConfiguration,
                                              ProductionLoadVolumeConfiguration,
                                              TPoolType, make_configuration)
from . import __version__


class LoaderClientConfiguration(BaseConfiguration):
    CLIENT_TYPE: str = None  # which destination to load data to


class LoaderClientDwhConfiguration(LoaderClientConfiguration):
    DEFAULT_DATASET: str = None  # dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix
    DEFAULT_SCHEMA_NAME: Optional[str] = None  # name of default schema to be used to name effective dataset to load data to


class LoaderConfiguration(PoolRunnerConfiguration, LoadVolumeConfiguration, LoaderClientConfiguration):
    WORKERS: int = 20  # how many parallel loads can be executed
    # MAX_PARALLELISM: int = 20  # in 20 separate threads
    POOL_TYPE: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool


class ProductionLoaderConfiguration(ProductionLoadVolumeConfiguration, LoaderConfiguration):
    pass


def configuration(initial_values: StrAny = None) -> Type[LoaderConfiguration]:
    return make_configuration(LoaderConfiguration, ProductionLoaderConfiguration, initial_values=initial_values)
