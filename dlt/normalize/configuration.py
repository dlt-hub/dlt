from dlt.common.typing import StrAny
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.configuration import (PoolRunnerConfiguration, NormalizeVolumeConfiguration,
                                              LoadVolumeConfiguration, SchemaVolumeConfiguration,
                                              TPoolType, make_configuration, configspec)

from . import __version__


@configspec
class NormalizeConfiguration(PoolRunnerConfiguration, NormalizeVolumeConfiguration, LoadVolumeConfiguration, SchemaVolumeConfiguration):
    loader_file_format: TLoaderFileFormat = "jsonl"  # jsonp or insert commands will be generated
    pool_type: TPoolType = "process"


def configuration(initial_values: StrAny = None) -> NormalizeConfiguration:
    return make_configuration(NormalizeConfiguration(), initial_value=initial_values)
