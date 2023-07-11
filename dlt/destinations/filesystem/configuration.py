from urllib.parse import urlparse

from typing import Final, Type, Optional, Union

from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import CredentialsConfiguration, DestinationClientStagingConfiguration
from dlt.common.configuration.specs import GcpCredentials, GcpServiceAccountCredentials, AwsCredentials, GcpOAuthCredentials

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
