from dlt.common.configuration import configspec
from dlt.common.destination import DestinationClientConfiguration, TLoaderFileFormat


@configspec(init=True)
class DummyClientConfiguration(DestinationClientConfiguration):
    destination_name: str = "dummy"
    loader_file_format: TLoaderFileFormat = "jsonl"
    fail_prob: float = 0.0
    retry_prob: float = 0.0
    completed_prob: float = 0.0
    timeout: float = 10.0
