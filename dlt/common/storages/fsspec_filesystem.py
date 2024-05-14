import io
import glob
import gzip
import mimetypes
import pathlib
import posixpath
from io import BytesIO
from gzip import GzipFile
from typing import (
    Literal,
    cast,
    Tuple,
    TypedDict,
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

from fsspec import AbstractFileSystem, register_implementation
from fsspec.core import url_to_fs

from dlt import version
from dlt.common.pendulum import pendulum
from dlt.common.configuration.specs import (
    GcpCredentials,
    AwsCredentials,
    AzureCredentials,
)
from dlt.common.exceptions import MissingDependencyException
from dlt.common.storages.configuration import FileSystemCredentials, FilesystemConfiguration
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import DictStrAny


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
}
# Support aliases
MTIME_DISPATCH["gs"] = MTIME_DISPATCH["gcs"]
MTIME_DISPATCH["s3a"] = MTIME_DISPATCH["s3"]
MTIME_DISPATCH["abfs"] = MTIME_DISPATCH["az"]

# Map of protocol to a filesystem type
CREDENTIALS_DISPATCH: Dict[str, Callable[[FilesystemConfiguration], DictStrAny]] = {
    "s3": lambda config: cast(AwsCredentials, config.credentials).to_s3fs_credentials(),
    "adl": lambda config: cast(AzureCredentials, config.credentials).to_adlfs_credentials(),
    "az": lambda config: cast(AzureCredentials, config.credentials).to_adlfs_credentials(),
    "gcs": lambda config: cast(GcpCredentials, config.credentials).to_gcs_credentials(),
    "gs": lambda config: cast(GcpCredentials, config.credentials).to_gcs_credentials(),
    "gdrive": lambda config: {"credentials": cast(GcpCredentials, config.credentials)},
    "abfs": lambda config: cast(AzureCredentials, config.credentials).to_adlfs_credentials(),
    "azure": lambda config: cast(AzureCredentials, config.credentials).to_adlfs_credentials(),
}


def fsspec_filesystem(
    protocol: str,
    credentials: FileSystemCredentials = None,
    kwargs: Optional[DictStrAny] = None,
    client_kwargs: Optional[DictStrAny] = None,
) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

    Please supply credentials instance corresponding to the protocol.
    The `protocol` is just the code name of the filesystem i.e.:
    * s3
    * az, abfs
    * gcs, gs

    also see filesystem_from_config
    """
    return fsspec_from_config(
        FilesystemConfiguration(protocol, credentials, kwargs=kwargs, client_kwargs=client_kwargs)
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
    fs_kwargs: DictStrAny = {"use_listings_cache": False, "listings_expiry_time": 60.0}
    credentials = CREDENTIALS_DISPATCH.get(protocol, lambda _: {})(config)

    if protocol == "gdrive":
        from dlt.common.storages.fsspecs.google_drive import GoogleDriveFileSystem

        register_implementation("gdrive", GoogleDriveFileSystem, "GoogleDriveFileSystem")

    if config.kwargs is not None:
        fs_kwargs.update(config.kwargs)
    if config.client_kwargs is not None:
        fs_kwargs["client_kwargs"] = config.client_kwargs

    if "client_kwargs" in fs_kwargs and "client_kwargs" in credentials:
        fs_kwargs["client_kwargs"].update(credentials.pop("client_kwargs"))

    fs_kwargs.update(credentials)
    return fs_kwargs


def fsspec_from_config(config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` from `config` argument.

    Authenticates following filesystems:
    * s3
    * az, abfs
    * gcs, gs

    All other filesystems are not authenticated

    Returns: (fsspec filesystem, normalized url)
    """
    fs_kwargs = prepare_fsspec_args(config)

    try:
        return url_to_fs(config.bucket_url, **fs_kwargs)  # type: ignore
    except ModuleNotFoundError as e:
        raise MissingDependencyException(
            "filesystem", [f"{version.DLT_PKG_NAME}[{config.protocol}]"]
        ) from e


class FileItemDict(DictStrAny):
    """A FileItem dictionary with additional methods to get fsspec filesystem, open and read files."""

    def __init__(
        self,
        mapping: FileItem,
        credentials: Optional[Union[FileSystemCredentials, AbstractFileSystem]] = None,
    ):
        """Create a dictionary with the filesystem client.

        Args:
            mapping (FileItem): The file item TypedDict.
            credentials (Optional[FileSystemCredentials], optional): The credentials to the
                filesystem. Defaults to None.
        """
        self.credentials = credentials
        super().__init__(**mapping)

    @property
    def fsspec(self) -> AbstractFileSystem:
        """The filesystem client is based on the given credentials.

        Returns:
            AbstractFileSystem: The fsspec client.
        """
        if isinstance(self.credentials, AbstractFileSystem):
            return self.credentials
        else:
            return fsspec_filesystem(self["file_url"], self.credentials)[0]

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
            raise ValueError("""The argument `compression` must have one of the following values:
                "auto", "enable", "disable".""")

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
        bucket_url = FilesystemConfiguration.make_file_uri(bucket_url)
        bucket_url_parsed = urlparse(bucket_url)
    else:
        bucket_url_parsed = urlparse(bucket_url)

    if is_local_fs:
        root_dir = FilesystemConfiguration.make_local_path(bucket_url)
        # use a Python glob to get files
        files = glob.glob(str(pathlib.Path(root_dir).joinpath(file_glob)), recursive=True)
        glob_result = {file: fs_client.info(file) for file in files}
    else:
        root_dir = bucket_url_parsed._replace(scheme="", query="").geturl().lstrip("/")
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
        # relative paths are always POSIX
        if is_local_fs:
            rel_path = pathlib.Path(file).relative_to(root_dir).as_posix()
            file_url = FilesystemConfiguration.make_file_uri(file)
        else:
            rel_path = posixpath.relpath(file, root_dir)
            file_url = bucket_url_parsed._replace(
                path=posixpath.join(bucket_url_parsed.path, rel_path)
            ).geturl()

        scheme = bucket_url_parsed.scheme
        mime_type, encoding = guess_mime_type(rel_path)
        yield FileItem(
            file_name=posixpath.basename(rel_path),
            relative_path=rel_path,
            file_url=file_url,
            mime_type=mime_type,
            encoding=encoding,
            modification_date=MTIME_DISPATCH[scheme](md),
            size_in_bytes=int(md["size"]),
        )
