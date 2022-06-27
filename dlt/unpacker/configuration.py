from typing import Type

from dlt.common.typing import StrAny
from dlt.common.dataset_writers import TWriterType
from dlt.common.configuration import (PoolRunnerConfiguration, UnpackingVolumeConfiguration,
                                              LoadingVolumeConfiguration, SchemaVolumeConfiguration,
                                              ProductionLoadingVolumeConfiguration, ProductionUnpackingVolumeConfiguration,
                                              ProductionSchemaVolumeConfiguration,
                                              TPoolType, make_configuration)

from . import __version__


class UnpackerConfiguration(PoolRunnerConfiguration, UnpackingVolumeConfiguration, LoadingVolumeConfiguration, SchemaVolumeConfiguration):
    MAX_EVENTS_IN_CHUNK: int = 40000  # maximum events to be processed in single chunk
    WRITER_TYPE: TWriterType = "jsonl"  # jsonp or insert commands will be generated
    POOL_TYPE: TPoolType = "process"


class ProductionUnpackerConfiguration(ProductionUnpackingVolumeConfiguration, ProductionLoadingVolumeConfiguration,
                                      ProductionSchemaVolumeConfiguration, UnpackerConfiguration):
    pass


def configuration(initial_values: StrAny = None) -> Type[UnpackerConfiguration]:
    return make_configuration(UnpackerConfiguration, ProductionUnpackerConfiguration, initial_values=initial_values)
