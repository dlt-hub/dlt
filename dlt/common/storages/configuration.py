from dataclasses import dataclass
import os
import pathlib
import posixpath
from typing import Any, Literal, Optional, Type, ClassVar, Dict, Union
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
    SFTPCredentials,
)
from dlt.common.configuration.specs.base_configuration import NotResolved
from dlt.common.exceptions import TerminalValueError
from dlt.common.typing import Annotated, DictStrAny, DictStrOptionalStr, get_args
from dlt.common.utils import digest128


TSchemaFileFormat = Literal["json", "yaml", "dbml"]
SCHEMA_FILES_EXTENSIONS = get_args(TSchemaFileFormat)


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
    AwsCredentials,
    GcpServiceAccountCredentials,
    AnyAzureCredentials,
    GcpOAuthCredentials,
    SFTPCredentials,
]


def ensure_canonical_az_url(
    bucket_url: str, target_scheme: str, storage_account_name: str = None, account_host: str = None
) -> str:
    """Converts any of the forms of azure blob storage into canonical form of {target_scheme}://<container_name>@<storage_account_name>.{account_host}/path

    `azure_storage_account_name` is optional only if not present in bucket_url, `account_host` assumes "<azure_storage_account_name>.dfs.core.windows.net" by default
    """
    parsed_bucket_url = urlparse(bucket_url)
    # Converts an az://<container_name>/<path> to abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
    if parsed_bucket_url.username:
        # has the right form, ensure abfss schema
        return urlunparse(parsed_bucket_url._replace(scheme=target_scheme))

    if not storage_account_name and not account_host:
        raise TerminalValueError(
            f"Could not convert azure blob storage url `{bucket_url}` into canonical form "
            f" ({target_scheme}://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>)"
            f" because storage account name is not known. Please use {target_scheme}:// canonical"
            " url as bucket_url in filesystem credentials"
        )

    account_host = account_host or f"{storage_account_name}.dfs.core.windows.net"
    netloc = (
        f"{parsed_bucket_url.netloc}@{account_host}" if parsed_bucket_url.netloc else account_host
    )

    # as required by databricks
    _path = parsed_bucket_url.path
    return urlunparse(
        parsed_bucket_url._replace(
            scheme=target_scheme,
            netloc=netloc,
            path=_path,
        )
    )


def _make_sftp_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    parsed_bucket_url = urlparse(bucket_url)
    return f"{scheme}://{parsed_bucket_url.hostname}{fs_path}"


def _make_az_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    parsed_bucket_url = urlparse(bucket_url)
    if parsed_bucket_url.username:
        # az://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>
        # fs_path always starts with container
        split_path = fs_path.split("/", maxsplit=1)
        # preserve slash at the end
        if len(split_path) == 2 and split_path[1] == "":
            split_path[1] = "/"
        # if just a container name, add empty path
        if len(split_path) == 1:
            split_path.append("")
        container, path = split_path
        netloc = f"{container}@{parsed_bucket_url.hostname}"
        # this strips trailing slash
        uri = urlunparse(parsed_bucket_url._replace(path=path, scheme=scheme, netloc=netloc))
        return uri
    return f"{scheme}://{fs_path}"


def _make_file_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    """Creates a normalized file:// url from a local path

    netloc is never set. UNC paths are represented as file://host/path
    """
    p_ = pathlib.Path(fs_path)
    # will remove trailing separator
    p_ = p_.expanduser().resolve()
    uri = p_.as_uri()
    if fs_path.endswith(os.path.sep):
        uri += "/"
    return uri


MAKE_URI_DISPATCH = {"az": _make_az_url, "file": _make_file_url, "sftp": _make_sftp_url}

MAKE_URI_DISPATCH["adl"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["abfs"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["azure"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["abfss"] = MAKE_URI_DISPATCH["az"]
MAKE_URI_DISPATCH["local"] = MAKE_URI_DISPATCH["file"]


def make_fsspec_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    """Creates url from `fs_path` and `scheme` using bucket_url as an `url` template, if `fs_path`
    ends with separator (indicating folder), it is preserved

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
    * sftp
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
        "sftp": SFTPCredentials,
    }

    bucket_url: str = None

    # should be a union of all possible credentials as found in PROTOCOL_CREDENTIALS
    credentials: FileSystemCredentials = None

    read_only: bool = False
    """Indicates read only filesystem access. Will enable caching"""
    kwargs: Optional[DictStrAny] = None
    """Additional arguments passed to fsspec constructor ie. dict(use_ssl=True) for s3fs"""
    client_kwargs: Optional[DictStrAny] = None
    """Additional arguments passed to underlying fsspec native client ie. dict(verify="public.crt) for botocore"""
    config_kwargs: Optional[DictStrAny] = None
    """Additional arguments as Config in botocore"""
    deltalake_storage_options: Optional[DictStrAny] = None
    deltalake_configuration: Optional[DictStrOptionalStr] = None
    deltalake_streamed_exec: bool = True

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

    @property
    def pathlib(self) -> Any:
        """Returns pathlib suitable for joining and other path ops"""
        return os.path if self.is_local_path(self.bucket_url) else posixpath

    def on_resolved(self) -> None:
        self.verify_bucket_url()

    def on_partial(self) -> None:
        if self.bucket_url:
            self.verify_bucket_url()

    def normalize_bucket_url(self) -> None:
        """Normalizes bucket_url ie. by making local paths absolute and converting to file:"""
        # save original url
        self._orig_bucket_url = self.bucket_url
        # this is just a path in a local file system
        if self.is_local_path(self.bucket_url):
            self.bucket_url = self.make_file_url(self.bucket_url)

    def verify_bucket_url(self) -> None:
        url = urlparse(self.bucket_url)
        if not url.path and not url.netloc:
            raise ConfigurationValueError(
                "File `path` and `netloc` are missing. Field `bucket_url` of"
                " `FilesystemClientConfiguration` must contain valid url with a path or"
                " host:password component."
            )
        self.normalize_bucket_url()

    def original_bucket_url(self) -> str:
        """Returns bucket_url before normalization"""
        return self._orig_bucket_url

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
            raise ValueError(f"Must be file scheme but is `{url.scheme}`")
        if not url.path and not url.netloc:
            raise ConfigurationValueError("File `path` and `netloc` are missing.")
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


@dataclass
class WithLocalFiles(BaseConfiguration):
    """Mixin to BaseConfiguration that shifts relative locations into `local_dir` and allows for a few special locations.
    :pipeline: in the pipeline working folder
    :external: location is an instance of an object (ie. fsspec or duckdb connection)

    Unifies default names for objects stored in filesystem (datasets / database names)
    - if destination_name is known it will be used to create name OR
    - if pipeline_name is known it will be used to create name

    Note: if not provided explicitly, `local_dir` is taken from run context and destination_name is injected
    Pipeline class which instantiates configuration will bind all NotResolved() params below explicitly

    """

    destination_name: Optional[str] = None

    local_dir: Annotated[str, NotResolved()] = None
    # needed by duckdb
    # TODO: deprecate this in 2.0
    pipeline_name: Annotated[Optional[str], NotResolved()] = None
    pipeline_working_dir: Annotated[Optional[str], NotResolved()] = None
    legacy_db_path: Annotated[Optional[str], NotResolved()] = None

    def on_partial(self) -> None:
        from dlt.common.runtime.run_context import active

        if not self.local_dir:
            self.local_dir = os.path.abspath(active().local_dir)
            if not self.is_partial():
                self.resolve()

    def make_location(self, configured_location: str, default_location_pat: str) -> str:
        # do not set any paths for external database / instance
        if configured_location == ":external:":
            return configured_location

        def _default_location() -> str:
            # use destination name to create default location name
            if self.destination_name:
                return default_location_pat % self.destination_name
            else:
                return default_location_pat % (self.pipeline_name or "")

        if configured_location == ":pipeline:":
            # try the pipeline context
            if self.pipeline_working_dir:
                return os.path.join(self.pipeline_working_dir, _default_location())
            raise RuntimeError(
                "Attempting to use special location `:pipeline:` outside of pipeline context."
            )
        else:
            # if explicit path is absolute, use it
            if configured_location and os.path.isabs(configured_location):
                return configured_location
            # TODO: restore this check for the paths below. we may require that path exists
            #  if pipeline had already run
            # if not self.bound_to_pipeline.first_run:
            #     if not os.path.exists(pipeline_path):
            #         logger.warning(
            #             f"Duckdb attached to pipeline {self.bound_to_pipeline.pipeline_name} in"
            #             f" path {os.path.relpath(pipeline_path)} was could not be found but"
            #             " pipeline has already ran. This may be a result of (1) recreating or"
            #             " attaching pipeline  without or with changed explicit path to database"
            #             " that was used when creating the pipeline. (2) keeping the path to to"
            #             " database in secrets and changing the current working folder so  dlt"
            #             " cannot see them. (3) you deleting the database."
            # use stored path if relpath
            if self.legacy_db_path:
                return self.legacy_db_path
            # use tmp path as root, not cwd
            return os.path.join(self.local_dir, configured_location or _default_location())


@configspec
class FilesystemConfigurationWithLocalFiles(FilesystemConfiguration, WithLocalFiles):
    """FilesystemConfiguration that adjust relative local filesystem bucket_url to
    be relative to `local_dir`.
    """

    def normalize_bucket_url(self) -> None:
        # here we deal with normalized file:// local paths
        if self.is_local_filesystem:
            # convert to native path
            try:
                local_file_path = self.make_local_path(self.bucket_url)
            except ValueError:
                local_file_path = self.bucket_url
            relocated_path = self.make_location(local_file_path, "%s")
            # convert back into file:// schema if relocated
            if local_file_path != relocated_path:
                if self.bucket_url.startswith("file:"):
                    self.bucket_url = self.make_file_url(relocated_path)
                else:
                    self.bucket_url = relocated_path
        # modified local path before it is normalized
        super().normalize_bucket_url()

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return super().resolve_credentials_type()

    @staticmethod
    def make_file_url(local_path: str) -> str:
        """Creates a normalized file:// url from a local path. If path is relative, `local_dir`
        will be used.
        netloc is never set. UNC paths are represented as file://host/path
        """
        if not os.path.isabs(local_path):
            from dlt.common.runtime.run_context import active

            local_path = os.path.join(active().local_dir, local_path)

        return make_fsspec_url("file", local_path, None)
