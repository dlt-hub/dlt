import dataclasses
from typing import Final, Type, Optional

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.storages import FilesystemConfiguration


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfiguration, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(default="filesystem", init=False, repr=False, compare=False)  # type: ignore

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]
