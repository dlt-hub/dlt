from dlt.common.typing import StrAny
from dlt.common.configuration import make_configuration, configspec
from dlt.common.data_writers import TLoaderFileFormat

from dlt.load.configuration import LoaderClientConfiguration


@configspec
class DummyClientConfiguration(LoaderClientConfiguration):
    client_type: str = "dummy"
    loader_file_format: TLoaderFileFormat = "jsonl"
    fail_prob: float = 0.0
    retry_prob: float = 0.0
    completed_prob: float = 0.0
    timeout: float = 10.0


def configuration(initial_values: StrAny = None) -> DummyClientConfiguration:
    return make_configuration(DummyClientConfiguration(), initial_value=initial_values)
