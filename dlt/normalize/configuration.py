from dlt.common.typing import StrAny
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.configuration import make_configuration, configspec
from dlt.common.configuration.specs import PoolRunnerConfiguration, TPoolType


@configspec(init=True)
class NormalizeConfiguration(PoolRunnerConfiguration):
    loader_file_format: TLoaderFileFormat = "jsonl"  # jsonp or insert commands will be generated
    pool_type: TPoolType = "process"
