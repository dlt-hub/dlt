from typing import TYPE_CHECKING, Optional

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import DestinationClientConfiguration, CredentialsConfiguration


@configspec
class DummyClientCredentials(CredentialsConfiguration):

    def __str__(self) -> str:
        return "/dev/null"


@configspec
class DummyClientConfiguration(DestinationClientConfiguration):
    destination_name: str = "dummy"
    loader_file_format: TLoaderFileFormat = "jsonl"
    fail_schema_update: bool = False
    fail_prob: float = 0.0
    retry_prob: float = 0.0
    completed_prob: float = 0.0
    timeout: float = 10.0
    fail_in_init: bool = True

    credentials: DummyClientCredentials = None

    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Optional[CredentialsConfiguration] = None,
            loader_file_format: TLoaderFileFormat = None,
            fail_schema_update: bool = None,
            fail_prob: float = None,
            retry_prob: float = None,
            completed_prob: float = None,
            timeout: float = None,
            fail_in_init: bool = None,
        ) -> None:
            ...
