import io
import mimetypes
import posixpath
import pathlib
from urllib.parse import urlparse
from io import BytesIO
from typing import cast, Tuple, TypedDict, Optional, Union, Iterator, Any, IO

from fsspec.core import url_to_fs
from fsspec import AbstractFileSystem

from dlt.common import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import DictStrAny
from dlt.common.configuration.specs import (
    CredentialsWithDefault,
    GcpCredentials,
    AwsCredentials,
    AzureCredentials,
)
from dlt.common.storages.configuration import FileSystemCredentials, FilesystemConfiguration

from dlt import version


class FileItem(TypedDict, total=False):
    """A DataItem representing a file"""

    file_url: str
    file_name: str
    mime_type: str
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
}
# Support aliases
MTIME_DISPATCH["gs"] = MTIME_DISPATCH["gcs"]
MTIME_DISPATCH["s3a"] = MTIME_DISPATCH["s3"]
MTIME_DISPATCH["abfs"] = MTIME_DISPATCH["az"]


def fsspec_filesystem(
    protocol: str, credentials: FileSystemCredentials = None
) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

    Please supply credentials instance corresponding to the protocol. The `protocol` is just the code name of the filesystem ie:
    * s3
    * az, abfs
    * gcs, gs

    also see filesystem_from_config
    """
    return fsspec_from_config(FilesystemConfiguration(protocol, credentials))


def fsspec_from_config(config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` from `config` argument.

    Authenticates following filesystems:
    * s3
    * az, abfs
    * gcs, gs

    All other filesystems are not authenticated

    Returns: (fsspec filesystem, normalized url)

    """
    proto = config.protocol
    fs_kwargs: DictStrAny = {}
    if proto == "s3":
        fs_kwargs.update(cast(AwsCredentials, config.credentials).to_s3fs_credentials())
    elif proto in ["az", "abfs", "adl", "azure"]:
        fs_kwargs.update(cast(AzureCredentials, config.credentials).to_adlfs_credentials())
    elif proto in ["gcs", "gs"]:
        assert isinstance(config.credentials, GcpCredentials)
        # Default credentials are handled by gcsfs
        if (
            isinstance(config.credentials, CredentialsWithDefault)
            and config.credentials.has_default_credentials()
        ):
            fs_kwargs["token"] = None
        else:
            fs_kwargs["token"] = dict(config.credentials)
        fs_kwargs["project"] = config.credentials.project_id
    try:
        return url_to_fs(config.bucket_url, use_listings_cache=False, **fs_kwargs)  # type: ignore[no-any-return]
    except ModuleNotFoundError as e:
        raise MissingDependencyException("filesystem", [f"{version.DLT_PKG_NAME}[{proto}]"]) from e


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
        """The filesystem client based on the given credentials.

        Returns:
            AbstractFileSystem: The fsspec client.
        """
        if isinstance(self.credentials, AbstractFileSystem):
            return self.credentials
        else:
            return fsspec_filesystem(self["file_url"], self.credentials)[0]

    def open(self, mode: str = "rb", **kwargs: Any) -> IO[Any]:  # noqa: A003
        """Open the file as a fsspec file.

        This method opens the file represented by this dictionary as a file-like object using
        the fsspec library.

        Args:
            **kwargs (Any): The arguments to pass to the fsspec open function.

        Returns:
            IOBase: The fsspec file.
        """
        opened_file: IO[Any]
        # if the user has already extracted the content, we use it so there will be no need to
        # download the file again.
        if "file_content" in self:
            bytes_io = BytesIO(self["file_content"])

            if "t" in mode:
                text_kwargs = {
                    k: kwargs.pop(k) for k in ["encoding", "errors", "newline"] if k in kwargs
                }
                return io.TextIOWrapper(
                    bytes_io,
                    **text_kwargs,
                )
            else:
                return bytes_io
        else:
            opened_file = self.fsspec.open(self["file_url"], mode=mode, **kwargs)
        return opened_file

    def read_bytes(self) -> bytes:
        """Read the file content.

        Returns:
            bytes: The file content.
        """
        content: bytes
        # same as open, if the user has already extracted the content, we use it.
        if "file_content" in self and self["file_content"] is not None:
            content = self["file_content"]
        else:
            content = self.fsspec.read_bytes(self["file_url"])
        return content


def guess_mime_type(file_name: str) -> str:
    mime_type = mimetypes.guess_type(posixpath.basename(file_name), strict=False)[0]
    if not mime_type:
        mime_type = "application/" + (posixpath.splitext(file_name)[1][1:] or "octet-stream")
    return mime_type


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
    import os

    bucket_url_parsed = urlparse(bucket_url)
    # if this is file path without scheme
    if not bucket_url_parsed.scheme or (os.path.isabs(bucket_url) and "\\" in bucket_url):
        # this is a file so create a proper file url
        bucket_url = pathlib.Path(bucket_url).absolute().as_uri()
        bucket_url_parsed = urlparse(bucket_url)

    bucket_path = bucket_url_parsed._replace(scheme="").geturl()
    bucket_path = bucket_path[2:] if bucket_path.startswith("//") else bucket_path
    filter_url = posixpath.join(bucket_path, file_glob)

    glob_result = fs_client.glob(filter_url, detail=True)
    if isinstance(glob_result, list):
        raise NotImplementedError(
            "Cannot request details when using fsspec.glob. For ADSL (Azure) please use version"
            " 2023.9.0 or later"
        )

    for file, md in glob_result.items():
        if md["type"] != "file":
            continue
        # make that absolute path on a file://
        if bucket_url_parsed.scheme == "file" and not file.startswith("/"):
            file = "/" + file
        file_name = posixpath.relpath(file, bucket_path)
        file_url = bucket_url_parsed.scheme + "://" + file
        yield FileItem(
            file_name=file_name,
            file_url=file_url,
            mime_type=guess_mime_type(file_name),
            modification_date=MTIME_DISPATCH[bucket_url_parsed.scheme](md),
            size_in_bytes=int(md["size"]),
        )
