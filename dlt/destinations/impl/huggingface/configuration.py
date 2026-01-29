import dataclasses
from typing import Final, Optional
from typing_extensions import Annotated

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.client import DestinationClientDwhConfiguration


@configspec
class HfCredentials(CredentialsConfiguration):
    token: Optional[str] = None
    endpoint: Optional[str] = None


@configspec
class HfClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="huggingface", init=False, repr=False, compare=False
    )
    credentials: Optional[HfCredentials] = None
    enable_dataset_name_normalization: bool = False

    dataset_name: Annotated[str, NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )  # dataset cannot be resolved
    split: str = "train"
