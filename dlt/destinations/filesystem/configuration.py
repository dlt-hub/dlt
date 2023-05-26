from urllib.parse import urlparse

from typing import Final, Type, Optional, Union

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.reference import CredentialsConfiguration, DestinationClientDwhConfiguration
from dlt.common.configuration.specs import GcpCredentials, GcpServiceAccountCredentials, AwsCredentials, GcpOAuthCredentials


PROTOCOL_CREDENTIALS = {
    "gs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
    "gcs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
    "gdrive": GcpOAuthCredentials,
    "s3": AwsCredentials
}


@configspec(init=True)
class FilesystemClientConfiguration(DestinationClientDwhConfiguration):
    credentials: Optional[Union[GcpCredentials, AwsCredentials]]

    destination_name: Final[str] = "filesystem"  # type: ignore
    bucket_url: str

    @property
    def protocol(self) -> str:
        return urlparse(self.bucket_url).scheme

    @resolve_type('credentials')
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]
