import io
import glob
import gzip
import mimetypes
import pathlib
import posixpath
from io import BytesIO
from typing import (
    Literal,
    cast,
    Tuple,
    Optional,
    Union,
    Iterator,
    Any,
    IO,
    Dict,
    Callable,
    Sequence,
)
from urllib.parse import urlparse

from fsspec import AbstractFileSystem, register_implementation, get_filesystem_class
from fsspec.core import url_to_fs

from dlt import version
from dlt.common.typing import TypedDict
from dlt.common.pendulum import pendulum
from dlt.common.configuration.specs import (
    GcpCredentials,
    AwsCredentials,
    AzureCredentials,
    SFTPCredentials,
)
from dlt.common.exceptions import MissingDependencyException, ValueErrorWithKnownValues
from dlt.common.storages.configuration import (
    FileSystemCredentials,
    FilesystemConfiguration,
    make_fsspec_url,
)
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import DictStrAny
from dlt.common.utils import without_none


class FileItem(TypedDict, total=False):
    """A DataItem representing a file"""

    file_url: str
    file_name: str
    relative_path: str
    mime_type: str
    encoding: Optional[str]
    modification_date: pendulum.DateTime
    size_in_bytes: int
    file_content: Optional[bytes]


# Map of protocol to mtime resolver
# we only need to support a small finite set of protocols
MTIME_DISPATCH = {
    "s3": lambda f: ensure_pendulum_datetime(f["LastModified"]),
    "adl": lambda f: ensure_pendulum_datetime(f["LastModified"]),
    "az": lambda f: ensure_pendulum_datetime(f["last_modified"]),
    "gcs": lambda f: ensure_pendulum_datetime(f["updated"]),
    "file": lambda f: ensure_pendulum_datetime(f["mtime"]),
    "memory": lambda f: ensure_pendulum_datetime(f["created"]),
    "gdrive": lambda f: ensure_pendulum_datetime(f["modifiedTime"]),
    "sftp": lambda f: ensure_pendulum_datetime(f["mtime"]),
}
# Support aliases
MTIME_DISPATCH["gs"] = MTIME_DISPATCH["gcs"]
MTIME_DISPATCH["s3a"] = MTIME_DISPATCH["s3"]
MTIME_DISPATCH["abfs"] = MTIME_DISPATCH["az"]
MTIME_DISPATCH["abfss"] = MTIME_DISPATCH["az"]

# Map of protocol to a filesystem type
CREDENTIALS_DISPATCH: Dict[str, Callable[[FilesystemConfiguration], DictStrAny]] = {
    "s3": lambda config: cast(AwsCredentials, config.credentials).to_s3fs_credentials(),
    "az": lambda config: cast(AzureCredentials, config.credentials).to_adlfs_credentials(),
    "gs": lambda config: cast(GcpCredentials, config.credentials).to_gcs_credentials(),
    "gdrive": lambda config: {"credentials": cast(GcpCredentials, config.credentials)},
    "sftp": lambda config: cast(SFTPCredentials, config.credentials).to_fsspec_credentials(),
}
CREDENTIALS_DISPATCH["adl"] = CREDENTIALS_DISPATCH["az"]
CREDENTIALS_DISPATCH["abfs"] = CREDENTIALS_DISPATCH["az"]
CREDENTIALS_DISPATCH["azure"] = CREDENTIALS_DISPATCH["az"]
CREDENTIALS_DISPATCH["abfss"] = CREDENTIALS_DISPATCH["az"]
CREDENTIALS_DISPATCH["gcs"] = CREDENTIALS_DISPATCH["gs"]

# Default kwargs for protocol
DEFAULT_KWARGS = {
    # disable concurrent
    "az": {"max_concurrency": 1}
}
DEFAULT_KWARGS["adl"] = DEFAULT_KWARGS["az"]
DEFAULT_KWARGS["abfs"] = DEFAULT_KWARGS["az"]
DEFAULT_KWARGS["azure"] = DEFAULT_KWARGS["az"]
DEFAULT_KWARGS["abfss"] = DEFAULT_KWARGS["az"]

AZURE_BLOB_STORAGE_PROTOCOLS = ["az", "azure", "adl", "abfss", "abfs"]
S3_PROTOCOLS = ["s3", "s3a"]
GCS_PROTOCOLS = ["gs", "gcs"]


def fsspec_filesystem(
    protocol: str,
    credentials: FileSystemCredentials = None,
    kwargs: Optional[DictStrAny] = None,
    client_kwargs: Optional[DictStrAny] = None,
    config_kwargs: Optional[DictStrAny] = None,
) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

    Please supply credentials instance corresponding to the protocol.
    The `protocol` is just the code name of the filesystem i.e.:
    * s3
    * az, abfs, abfss, adl, azure
    * gcs, gs

    also see filesystem_from_config
    """
    return fsspec_from_config(
        FilesystemConfiguration(
            protocol,
            credentials,
            kwargs=kwargs,
            client_kwargs=client_kwargs,
            config_kwargs=config_kwargs,
        )
    )


def prepare_fsspec_args(config: FilesystemConfiguration) -> DictStrAny:
    """Prepare arguments for fsspec filesystem constructor.

    Args:
        config (FilesystemConfiguration): The filesystem configuration.

    Returns:
        DictStrAny: The arguments for the fsspec filesystem constructor.
    """
    protocol = config.protocol
    # never use listing caches
    fs_kwargs: DictStrAny = {
        "use_listings_cache": config.read_only,
        "listings_expiry_time": 60.0,
        "skip_instance_cache": False,
    }
    credentials = CREDENTIALS_DISPATCH.get(protocol, lambda _: {})(config)

    if protocol == "gdrive":
        from dlt.common.storages.fsspecs.google_drive import GoogleDriveFileSystem

        register_implementation("gdrive", GoogleDriveFileSystem, "GoogleDriveFileSystem")

    fs_kwargs.update(DEFAULT_KWARGS.get(protocol, {}))

    if protocol == "sftp":
        fs_kwargs.clear()

    if config.kwargs is not None:
        fs_kwargs.update(config.kwargs)
    if config.client_kwargs is not None:
        fs_kwargs["client_kwargs"] = config.client_kwargs

    if "client_kwargs" in fs_kwargs and "client_kwargs" in credentials:
        fs_kwargs["client_kwargs"].update(credentials.pop("client_kwargs"))

    if "config_kwargs" in fs_kwargs and "config_kwargs" in credentials:
        fs_kwargs["config_kwargs"].update(credentials.pop("config_kwargs"))

    fs_kwargs.update(without_none(credentials))
    return fs_kwargs


def fsspec_from_config(config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` from `config` argument.

    Authenticates following filesystems:
    * s3
    * az, abfs, abfss, adl, azure
    * gcs, gs
    * sftp

    All other filesystems are not authenticated

    Returns: (fsspec filesystem, normalized url)
    """
    fs_kwargs = prepare_fsspec_args(config)

    try:
        # first get the class to check the protocol
        fs_cls = get_filesystem_class(config.protocol)
        if fs_cls.protocol == "abfs":
            url = urlparse(config.bucket_url)
            # if storage account is present in bucket_url and in credentials, az fsspec will fail
            # account name is detected only for blob.core.windows.net host
            if url.username and (
                url.hostname.endswith("blob.core.windows.net")
                or url.hostname.endswith("dfs.core.windows.net")
            ):
                fs_kwargs.pop("account_name")
        return url_to_fs(config.bucket_url, **fs_kwargs)  # type: ignore
    except ImportError as e:
        raise MissingDependencyException(
            "filesystem", [f"{version.DLT_PKG_NAME}[{config.protocol}]"]
        ) from e


class FileItemDict(DictStrAny):
    """A FileItem dictionary with additional methods to get fsspec filesystem, open and read files."""

    def __init__(
        self,
        mapping: FileItem,
        fsspec: AbstractFileSystem = None,
    ):
        """Create a dictionary with the filesystem client.

        Args:
            mapping (FileItem): The file item TypedDict.
            credentials (Optional[FileSystemCredentials], optional): The credentials to the
                filesystem. Defaults to None.
        """
        self._fsspec = fsspec
        super().__init__(**mapping)

    @property
    def fsspec(self) -> AbstractFileSystem:
        """The filesystem client is based on the given credentials.

        Returns:
            AbstractFileSystem: The fsspec client.
        """
        return self._fsspec

    @property
    def local_file_path(self) -> str:
        """Gets a valid local filesystem path from file:// scheme.
        Supports POSIX/Windows/UNC paths

        Returns:
            str: local filesystem path
        """
        return FilesystemConfiguration.make_local_path(self["file_url"])

    def open(  # noqa: A003
        self,
        mode: str = "rb",
        compression: Literal["auto", "disable", "enable"] = "auto",
        **kwargs: Any,
    ) -> IO[Any]:
        """Open the file as a fsspec file.

        This method opens the file represented by this dictionary as a file-like object using
        the fsspec library.

        Args:
            mode (Optional[str]): Open mode.
            compression (Optional[str]): A flag to enable/disable compression.
                Can have one of three values: "disable" - no compression applied,
                "enable" - gzip compression applied, "auto" (default) -
                compression applied only for files compressed with gzip.
            **kwargs (Any): The arguments to pass to the fsspec open function.

        Returns:
            IOBase: The fsspec file.
        """
        if compression == "auto":
            compression_arg = "gzip" if self["encoding"] == "gzip" else None
        elif compression == "enable":
            compression_arg = "gzip"
        elif compression == "disable":
            compression_arg = None
        else:
            raise ValueErrorWithKnownValues(
                "compression", compression, ["auto", "enable", "disable"]
            )

        # if the user has already extracted the content, we use it so there is no need to
        # download the file again.
        if "file_content" in self:
            content = (
                gzip.decompress(self["file_content"])
                if compression_arg == "gzip"
                else self["file_content"]
            )
            bytes_io = BytesIO(content)

            if "t" not in mode:
                return bytes_io
            text_kwargs = {
                k: kwargs.pop(k) for k in ["encoding", "errors", "newline"] if k in kwargs
            }
            return io.TextIOWrapper(
                bytes_io,
                **text_kwargs,
            )
        else:
            if "file" in self.fsspec.protocol:
                # use native local file path to open file:// uris
                file_url = self.local_file_path
            else:
                file_url = self["file_url"]
            return self.fsspec.open(  # type: ignore[no-any-return]
                file_url, mode=mode, compression=compression_arg, **kwargs
            )

    def read_bytes(self) -> bytes:
        """Read the file content.

        Returns:
            bytes: The file content.
        """
        if "file_content" in self and self["file_content"] is not None:
            return self["file_content"]  # type: ignore
        else:
            with self.open(mode="rb", compression="disable") as f:
                return f.read()  # type: ignore[no-any-return]


def guess_mime_type(file_name: str) -> Sequence[str]:
    type_ = list(mimetypes.guess_type(posixpath.basename(file_name), strict=False))

    if not type_[0]:
        type_[0] = "application/" + (posixpath.splitext(file_name)[1][1:] or "octet-stream")

    return type_


def glob_files(
    fs_client: AbstractFileSystem, bucket_url: str, file_glob: str = "**"
) -> Iterator[FileItem]:
    """Get the files from the filesystem client.

    Args:
        fs_client (AbstractFileSystem): The filesystem client.
        bucket_url (str): The url to the bucket.
        file_glob (str): A glob for the filename filter.

    Returns:
        Iterable[FileItem]: The list of files.
    """
    is_local_fs = "file" in fs_client.protocol
    if is_local_fs and FilesystemConfiguration.is_local_path(bucket_url):
        bucket_url = FilesystemConfiguration.make_file_url(bucket_url)
    bucket_url_parsed = urlparse(bucket_url)

    if is_local_fs:
        root_dir = FilesystemConfiguration.make_local_path(bucket_url)
        # use a Python glob to get files
        files = glob.glob(str(pathlib.Path(root_dir).joinpath(file_glob)), recursive=True)
        glob_result = {file: fs_client.info(file) for file in files}
    else:
        # convert to fs_path
        root_dir = fs_client._strip_protocol(bucket_url)
        filter_url = posixpath.join(root_dir, file_glob)
        glob_result = fs_client.glob(filter_url, detail=True)
        if isinstance(glob_result, list):
            raise NotImplementedError(
                "Cannot request details when using fsspec.glob. For adlfs (Azure) please use"
                " version 2023.9.0 or later"
            )

    for file, md in glob_result.items():
        if md["type"] != "file":
            continue
        scheme = bucket_url_parsed.scheme

        # relative paths are always POSIX
        if is_local_fs:
            # use OS pathlib for local paths
            loc_path = pathlib.Path(file)
            file_name = loc_path.name
            rel_path = loc_path.relative_to(root_dir).as_posix()
            file_url = FilesystemConfiguration.make_file_url(file)
        else:
            file_name = posixpath.basename(file)
            rel_path = posixpath.relpath(file, root_dir)
            file_url = make_fsspec_url(scheme, file, bucket_url)

        mime_type, encoding = guess_mime_type(rel_path)
        yield FileItem(
            file_name=file_name,
            relative_path=rel_path,
            file_url=file_url,
            mime_type=mime_type,
            encoding=encoding,
            modification_date=MTIME_DISPATCH[scheme](md),
            size_in_bytes=int(md["size"]),
        )
