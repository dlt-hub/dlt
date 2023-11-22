import os
from urllib.parse import urlparse
from typing import TYPE_CHECKING, Any, Literal, Optional, Type, get_args, ClassVar, Dict, Union

from dlt.common.configuration.specs import BaseConfiguration, configspec, CredentialsConfiguration
from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    AwsCredentials,
    GcpOAuthCredentials,
    AzureCredentials,
    AzureCredentialsWithoutDefaults,
    BaseConfiguration,
)
from dlt.common.utils import digest128
from dlt.common.configuration.exceptions import ConfigurationValueError

TSchemaFileFormat = Literal["json", "yaml"]
SchemaFileExtensions = get_args(TSchemaFileFormat)


@configspec
class SchemaStorageConfiguration(BaseConfiguration):
    schema_volume_path: str = None  # path to volume with default schemas
    import_schema_path: Optional[str] = None  # path from which to import a schema into storage
    export_schema_path: Optional[str] = None  # path to which export schema from storage
    external_schema_format: TSchemaFileFormat = "yaml"  # format in which to expect external schema
    external_schema_format_remove_defaults: bool = (
        True  # remove default values when exporting schema
    )

    if TYPE_CHECKING:

        def __init__(
            self,
            schema_volume_path: str = None,
            import_schema_path: str = None,
            export_schema_path: str = None,
        ) -> None: ...


@configspec
class NormalizeStorageConfiguration(BaseConfiguration):
    normalize_volume_path: str = None  # path to volume where normalized loader files will be stored

    if TYPE_CHECKING:

        def __init__(self, normalize_volume_path: str = None) -> None: ...


@configspec
class LoadStorageConfiguration(BaseConfiguration):
    load_volume_path: str = (
        None  # path to volume where files to be loaded to analytical storage are stored
    )
    delete_completed_jobs: bool = (
        False  # if set to true the folder with completed jobs will be deleted
    )

    if TYPE_CHECKING:

        def __init__(
            self, load_volume_path: str = None, delete_completed_jobs: bool = None
        ) -> None: ...


FileSystemCredentials = Union[
    AwsCredentials, GcpServiceAccountCredentials, AzureCredentials, GcpOAuthCredentials
]


@configspec
class FilesystemConfiguration(BaseConfiguration):
    """A configuration defining filesystem location and access credentials.

    When configuration is resolved, `bucket_url` is used to extract a protocol and request corresponding credentials class.
    * s3
    * gs, gcs
    * az, abfs, adl
    * file, memory
    * gdrive
    """

    PROTOCOL_CREDENTIALS: ClassVar[Dict[str, Any]] = {
        "gs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
        "gcs": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
        "gdrive": GcpOAuthCredentials,
        "s3": AwsCredentials,
        "az": Union[AzureCredentialsWithoutDefaults, AzureCredentials],
        "abfs": Union[AzureCredentialsWithoutDefaults, AzureCredentials],
        "adl": Union[AzureCredentialsWithoutDefaults, AzureCredentials],
    }

    bucket_url: str = None
    # should be an union of all possible credentials as found in PROTOCOL_CREDENTIALS
    credentials: FileSystemCredentials

    @property
    def protocol(self) -> str:
        """`bucket_url` protocol"""
        url = urlparse(self.bucket_url)
        # this prevents windows absolute paths to be recognized as schemas
        if not url.scheme or (os.path.isabs(self.bucket_url) and "\\" in self.bucket_url):
            return "file"
        else:
            return url.scheme

    def on_resolved(self) -> None:
        url = urlparse(self.bucket_url)
        if not url.path and not url.netloc:
            raise ConfigurationValueError(
                "File path or netloc missing. Field bucket_url of FilesystemClientConfiguration"
                " must contain valid url with a path or host:password component."
            )
        # this is just a path in local file system
        if url.path == self.bucket_url:
            url = url._replace(scheme="file")
            self.bucket_url = url.geturl()

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]

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

        def __init__(self, bucket_url: str, credentials: FileSystemCredentials = None) -> None: ...
