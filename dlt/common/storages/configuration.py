import os
import pathlib
from typing import Any, Literal, Optional, Type, get_args, ClassVar, Dict, Union
from urllib.parse import urlparse, unquote

from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    AwsCredentials,
    GcpOAuthCredentials,
    AnyAzureCredentials,
    BaseConfiguration,
)
from dlt.common.typing import DictStrAny
from dlt.common.utils import digest128


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


@configspec
class NormalizeStorageConfiguration(BaseConfiguration):
    normalize_volume_path: str = None  # path to volume where normalized loader files will be stored


@configspec
class LoadStorageConfiguration(BaseConfiguration):
    load_volume_path: str = (
        None  # path to volume where files to be loaded to analytical storage are stored
    )
    delete_completed_jobs: bool = (
        False  # if set to true the folder with completed jobs will be deleted
    )


FileSystemCredentials = Union[
    AwsCredentials, GcpServiceAccountCredentials, AnyAzureCredentials, GcpOAuthCredentials
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
        "gdrive": Union[GcpServiceAccountCredentials, GcpOAuthCredentials],
        "s3": AwsCredentials,
        "az": AnyAzureCredentials,
        "abfs": AnyAzureCredentials,
        "adl": AnyAzureCredentials,
    }

    bucket_url: str = None

    # should be a union of all possible credentials as found in PROTOCOL_CREDENTIALS
    credentials: FileSystemCredentials = None

    read_only: bool = False
    """Indicates read only filesystem access. Will enable caching"""
    kwargs: Optional[DictStrAny] = None
    client_kwargs: Optional[DictStrAny] = None

    @property
    def protocol(self) -> str:
        """`bucket_url` protocol"""
        if self.is_local_path(self.bucket_url):
            return "file"
        else:
            return urlparse(self.bucket_url).scheme

    def on_resolved(self) -> None:
        uri = urlparse(self.bucket_url)
        if not uri.path and not uri.netloc:
            raise ConfigurationValueError(
                "File path and netloc are missing. Field bucket_url of"
                " FilesystemClientConfiguration must contain valid uri with a path or host:password"
                " component."
            )
        # this is just a path in a local file system
        if self.is_local_path(self.bucket_url):
            self.bucket_url = self.make_file_uri(self.bucket_url)

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]

    def fingerprint(self) -> str:
        """Returns a fingerprint of bucket_url"""
        return digest128(self.bucket_url) if self.bucket_url else ""

    def __str__(self) -> str:
        """Return displayable destination location"""
        uri = urlparse(self.bucket_url)
        # do not show passwords
        if uri.password:
            new_netloc = f"{uri.username}:****@{uri.hostname}"
            if uri.port:
                new_netloc += f":{uri.port}"
            return uri._replace(netloc=new_netloc).geturl()
        return self.bucket_url

    @staticmethod
    def is_local_path(uri: str) -> bool:
        """Checks if `uri` is a local path, without a schema"""
        uri_parsed = urlparse(uri)
        # this prevents windows absolute paths to be recognized as schemas
        return not uri_parsed.scheme or os.path.isabs(uri)

    @staticmethod
    def make_local_path(file_uri: str) -> str:
        """Gets a valid local filesystem path from file:// scheme.
        Supports POSIX/Windows/UNC paths

        Returns:
            str: local filesystem path
        """
        uri = urlparse(file_uri)
        if uri.scheme != "file":
            raise ValueError(f"Must be file scheme but is {uri.scheme}")
        if not uri.path and not uri.netloc:
            raise ConfigurationValueError("File path and netloc are missing.")
        local_path = unquote(uri.path)
        if uri.netloc:
            # or UNC file://localhost/path
            local_path = "//" + unquote(uri.netloc) + local_path
        else:
            # if we are on windows, strip the POSIX root from path which is always absolute
            if os.path.sep != local_path[0]:
                # filesystem root
                if local_path == "/":
                    return str(pathlib.Path("/").resolve())
                # this prevents /C:/ or ///share/ where both POSIX and Windows root are present
                if os.path.isabs(local_path[1:]):
                    local_path = local_path[1:]
        return str(pathlib.Path(local_path))

    @staticmethod
    def make_file_uri(local_path: str) -> str:
        """Creates a normalized file:// uri from a local path

        netloc is never set. UNC paths are represented as file://host/path
        """
        p_ = pathlib.Path(local_path)
        p_ = p_.expanduser().resolve()
        return p_.as_uri()
