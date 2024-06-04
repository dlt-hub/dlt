import dataclasses
from typing import Final

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
    CredentialsConfiguration,
)


@configspec
class DummyClientCredentials(CredentialsConfiguration):
    def __str__(self) -> str:
        return "/dev/null"


@configspec
class DummyClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(default="dummy", init=False, repr=False, compare=False)  # type: ignore
    loader_file_format: TLoaderFileFormat = "jsonl"
    fail_schema_update: bool = False
    fail_prob: float = 0.0
    retry_prob: float = 0.0
    completed_prob: float = 0.0
    exception_prob: float = 0.0
    """probability of exception when checking job status"""
    timeout: float = 10.0
    fail_in_init: bool = True
    # new jobs workflows
    create_followup_jobs: bool = False

    credentials: DummyClientCredentials = None
