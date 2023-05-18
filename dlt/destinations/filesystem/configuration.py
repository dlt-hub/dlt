from urllib.parse import urlparse

from typing import Final, Type, NewType, Callable, TypeVar, Generic, Any, Optional, Union

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import DestinationClientConfiguration, CredentialsConfiguration, DestinationClientDwhConfiguration, BaseConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials, AwsCredentials, GcpOAuthCredentials


PROTOCOL_CREDENTIALS = {
    "gcs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
    "file": Optional[CredentialsConfiguration],  # Dummy hint
    "s3": AwsCredentials
}




@configspec(init=True)
class FilesystemClientConfiguration(DestinationClientDwhConfiguration):
    credentials: Optional[Union[GcpServiceAccountCredentials, AwsCredentials]]

    destination_name: Final[str] = "filesystem"  # type: ignore
    loader_file_format: TLoaderFileFormat = "jsonl"
    bucket_url: str = 'file://tmp'

    @property
    def protocol(self) -> str:
        return urlparse(self.bucket_url).scheme

    @resolve_type('credentials')
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return PROTOCOL_CREDENTIALS[self.protocol]
