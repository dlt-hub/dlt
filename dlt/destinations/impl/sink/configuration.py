from typing import TYPE_CHECKING, Optional, Final

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import (
    DestinationClientConfiguration,
    CredentialsConfiguration,
)


@configspec
class SinkClientCredentials(CredentialsConfiguration):
    callable_name: str = None


@configspec
class SinkClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = "sink"  # type: ignore
    credentials: SinkClientCredentials = None

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Optional[CredentialsConfiguration] = None,
        ) -> None: ...
