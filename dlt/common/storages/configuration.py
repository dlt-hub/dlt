import os
import pathlib
from typing import Any, Literal, Optional, Type, get_args, ClassVar, Dict, Union
from urllib.parse import urlparse, unquote, urlunparse

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


def _make_az_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    parsed_bucket_url = urlparse(bucket_url)
    if parsed_bucket_url.username:
        # az://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
        # fs_path always starts with container
        split_path = fs_path.split("/", maxsplit=1)
        if len(split_path) == 1:
            split_path.append("")
        container, path = split_path
        netloc = f"{container}@{parsed_bucket_url.hostname}"
        return urlunparse(parsed_bucket_url._replace(path=path, scheme=scheme, netloc=netloc))
    return f"{scheme}://{fs_path}"


def _make_file_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    """Creates a normalized file:// url from a local path

    netloc is never set. UNC paths are represented as file://host/path
    """
    p_ = pathlib.Path(fs_path)
    p_ = p_.expanduser().resolve()
    return p_.as_uri()


MAKE_URI_DISPATCH = {"az": _make_az_url, "file": _make_file_url}

MAKE_URI_DISPATCH["adl"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["abfs"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["azure"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["abfss"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["local"] = MAKE_URI_DISPATCH["file"]


def make_fsspec_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    """Creates url from `fs_path` and `scheme` using bucket_url as an `url` template

    Args:
        scheme (str): scheme of the resulting url
        fs_path (str): kind of absolute path that fsspec uses to locate resources for particular filesystem.
        bucket_url (str): an url template. the structure of url will be preserved if possible
    """
    _maker = MAKE_URI_DISPATCH.get(scheme)
    if _maker:
        return _maker(scheme, fs_path, bucket_url)
    return f"{scheme}://{fs_path}"


@configspec
class FilesystemConfiguration(BaseConfiguration):
    """A configuration defining filesystem location and access credentials.

    When configuration is resolved, `bucket_url` is used to extract a protocol and request corresponding credentials class.
    * s3
    * gs, gcs
    * az, abfs, adl, abfss, azure
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
        "abfss": AnyAzureCredentials,
        "azure": AnyAzureCredentials,
    }

    bucket_url: str = None

    # should be a union of all possible credentials as found in PROTOCOL_CREDENTIALS
    credentials: FileSystemCredentials = None

    read_only: bool = False
    """Indicates read only filesystem access. Will enable caching"""
    kwargs: Optional[DictStrAny] = None
    client_kwargs: Optional[DictStrAny] = None
    deltalake_storage_options: Optional[DictStrAny] = None

    @property
    def protocol(self) -> str:
        """`bucket_url` protocol"""
        if self.is_local_path(self.bucket_url):
            return "file"
        else:
            return urlparse(self.bucket_url).scheme

    @property
    def is_local_filesystem(self) -> bool:
        return self.protocol == "file"

    def on_resolved(self) -> None:
        url = urlparse(self.bucket_url)
        if not url.path and not url.netloc:
            raise ConfigurationValueError(
                "File path and netloc are missing. Field bucket_url of"
                " FilesystemClientConfiguration must contain valid url with a path or host:password"
                " component."
            )
        # this is just a path in a local file system
        if self.is_local_path(self.bucket_url):
            self.bucket_url = self.make_file_url(self.bucket_url)

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration]  # type: ignore[return-value]

    def fingerprint(self) -> str:
        """Returns a fingerprint of bucket schema and netloc.

        Returns:
            str: Fingerprint.
        """
        if not self.bucket_url:
            return ""

        if self.is_local_path(self.bucket_url):
            return digest128("")

        url = urlparse(self.bucket_url)
        return digest128(self.bucket_url.replace(url.path, ""))

    def make_url(self, fs_path: str) -> str:
        """Makes a full url (with scheme) form fs_path which is kind-of absolute path used by fsspec to identify resources.
        This method will use `bucket_url` to infer the original form of the url.
        """
        return make_fsspec_url(self.protocol, fs_path, self.bucket_url)

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

    @staticmethod
    def is_local_path(url: str) -> bool:
        """Checks if `url` is a local path, without a schema"""
        url_parsed = urlparse(url)
        # this prevents windows absolute paths to be recognized as schemas
        return not url_parsed.scheme or os.path.isabs(url)

    @staticmethod
    def make_local_path(file_url: str) -> str:
        """Gets a valid local filesystem path from file:// scheme.
        Supports POSIX/Windows/UNC paths

        Returns:
            str: local filesystem path
        """
        url = urlparse(file_url)
        if url.scheme != "file":
            raise ValueError(f"Must be file scheme but is {url.scheme}")
        if not url.path and not url.netloc:
            raise ConfigurationValueError("File path and netloc are missing.")
        local_path = unquote(url.path)
        if url.netloc:
            # or UNC file://localhost/path
            local_path = "//" + unquote(url.netloc) + local_path
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
    def make_file_url(local_path: str) -> str:
        """Creates a normalized file:// url from a local path

        netloc is never set. UNC paths are represented as file://host/path
        """
        return make_fsspec_url("file", local_path, None)
