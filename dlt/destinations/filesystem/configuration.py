from urllib.parse import urlparse

from typing import Final, Type, NewType, Callable, TypeVar, Generic, Any, Optional

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import DestinationClientConfiguration, CredentialsConfiguration, DestinationClientDwhConfiguration, BaseConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials


PROTOCOL_CREDENTIALS = {
    "gcs": GcpServiceAccountCredentials,
    "file": Optional[CredentialsConfiguration],  # Dummy hint
}


@configspec(init=True)
class FilesystemClientConfiguration(DestinationClientDwhConfiguration):
    credentials: CredentialsConfiguration

    destination_name: Final[str] = "filesystem"  # type: ignore
    loader_file_format: TLoaderFileFormat = "jsonl"
    bucket_url: str = 'file://tmp'


    @resolve_type('credentials')
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        scheme = urlparse(self.bucket_url).scheme
        return PROTOCOL_CREDENTIALS[scheme]
