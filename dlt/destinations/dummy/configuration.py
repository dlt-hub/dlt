from dlt.common.configuration import configspec
from dlt.common.destination import DestinationClientConfiguration, CredentialsConfiguration, TLoaderFileFormat


@configspec
class DummyClientCredentials(CredentialsConfiguration):

    def __str__(self) -> str:
        return "/dev/null"


@configspec(init=True)
class DummyClientConfiguration(DestinationClientConfiguration):
    destination_name: str = "dummy"
    loader_file_format: TLoaderFileFormat = "jsonl"
    fail_schema_update: bool = False
    fail_prob: float = 0.0
    retry_prob: float = 0.0
    completed_prob: float = 0.0
    timeout: float = 10.0

    credentials: DummyClientCredentials = None
