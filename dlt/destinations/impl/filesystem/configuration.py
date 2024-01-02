from urllib.parse import urlparse

from typing import Final, Type, Optional, Any, TYPE_CHECKING

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.storages import FilesystemConfiguration


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfiguration, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = "filesystem"  # type: ignore

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            credentials: Optional[Any] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            bucket_url: str = None,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
