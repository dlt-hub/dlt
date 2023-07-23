from urllib.parse import urlparse

from typing import Final, Type, Optional, Union, TYPE_CHECKING

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import CredentialsConfiguration, DestinationClientStagingConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials, AwsCredentials, GcpOAuthCredentials
from dlt.common.utils import digest128
from dlt.common.configuration.exceptions import ConfigurationValueError


PROTOCOL_CREDENTIALS = {
    "gs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
    "gcs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
    "gdrive": GcpOAuthCredentials,
    "s3": AwsCredentials
}


@configspec(init=True)
class FilesystemClientConfiguration(DestinationClientStagingConfiguration):
    destination_name: Final[str] = "filesystem"  # type: ignore
    # should be an union of all possible credentials as found in PROTOCOL_CREDENTIALS
    credentials: Union[AwsCredentials, GcpServiceAccountCredentials, GcpOAuthCredentials]
    bucket_url: str

    @property
    def protocol(self) -> str:
        url = urlparse(self.bucket_url)
        return url.scheme or "file"

    def on_resolved(self) -> None:
        url = urlparse(self.bucket_url)
        # print(url)
        if not url.path and not url.netloc:
            raise ConfigurationValueError("File path or netloc missing. Field bucket_url of FilesystemClientConfiguration must contain valid url with a path or host:password component.")
        # this is just a path in local file system
        if url.path == self.bucket_url:
            url = url._replace(scheme="file")
            self.bucket_url = url.geturl()

    @resolve_type('credentials')
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]

    def fingerprint(self) -> str:
        """Returns a fingerprint of bucket_url"""
        if self.bucket_url:
            return digest128(self.bucket_url)
        return ""

    def __str__(self) -> str:
        """Return displayable destination location"""
        url = urlparse(self.bucket_url)
        # do not show passwords
        if url.password:
            new_netloc = f"{url.username}:****@{url.hostname}"
            if url.port:
                new_netloc += f":{url.port}"
            return url._replace(netloc=new_netloc).geturl()
        return self.bucket_url

    if TYPE_CHECKING:
        def __init__(
            self,
            destination_name: str = None,
            credentials: Optional[GcpServiceAccountCredentials] = None,
            dataset_name: str = None,
            default_schema_name: Optional[str] = None,
            bucket_url: str = None
        ) -> None:
            ...
