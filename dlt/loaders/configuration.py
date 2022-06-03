from typing import Any, Type

from dlt.common.utils import uniq_id
from dlt.common.typing import StrAny
from dlt.common.configuration import (PoolRunnerConfiguration,
                                              LoadingVolumeConfiguration,
                                              ProductionLoadingVolumeConfiguration,
                                              PostgresConfiguration, PostgresProductionConfiguration,
                                              GcpClientConfiguration, GcpClientProductionConfiguration,
                                              TPoolType, make_configuration)

from dlt.loaders.dummy.configuration import DummyClientConfiguration

from . import __version__

class LoaderConfiguration(PoolRunnerConfiguration, LoadingVolumeConfiguration):
    CLIENT_TYPE: str = "dummy"  # which analytical storage to use
    MAX_PARALLEL_LOADS: int = 20  # how many parallel loads can be executed
    MAX_PARALLELISM: int = 20  # in 20 separate threads
    POOL_TYPE: TPoolType = "thread"  # mostly i/o (upload) so may be thread pool


class ProductionLoaderConfiguration(ProductionLoadingVolumeConfiguration, LoaderConfiguration):
    pass


def configuration(initial_values: StrAny = None) -> Type[LoaderConfiguration]:
    # synthesize right configuration
    C = make_configuration(LoaderConfiguration, ProductionLoaderConfiguration, initial_values=initial_values)
    T: Type[Any] = None
    T_P: Type[Any] = None
    if C.CLIENT_TYPE == "dummy":
        T = DummyClientConfiguration
        T_P = DummyClientConfiguration
    elif C.CLIENT_TYPE == "gcp":
        T = GcpClientConfiguration
        T_P = GcpClientProductionConfiguration
    elif C.CLIENT_TYPE == "redshift":
        T = PostgresConfiguration
        T_P = PostgresProductionConfiguration
    else:
        raise ValueError(C.CLIENT_TYPE)

    ST = type(LoaderConfiguration.__name__ + "_"  + T.__name__ + "_" + uniq_id(), (T, LoaderConfiguration), {})
    ST_P = type(ProductionLoaderConfiguration.__name__ + "_" + T_P.__name__ + "_" + uniq_id(), (T_P, ProductionLoaderConfiguration), {})
    return make_configuration(
        ST,
        ST_P,
        initial_values=initial_values,
        skip_subclass_check=True
    )
