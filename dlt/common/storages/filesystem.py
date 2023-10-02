from typing import cast, Tuple, TypedDict, Optional, Union

from fsspec.core import url_to_fs
from fsspec import AbstractFileSystem

from dlt.common import pendulum
from dlt.common.exceptions import MissingDependencyException
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import DictStrAny
from dlt.common.configuration.specs import CredentialsWithDefault, GcpCredentials, AwsCredentials, AzureCredentials
from dlt.common.storages.configuration import FileSystemCredentials, FilesystemConfiguration

from dlt import version


class FileItem(TypedDict):
    """A DataItem representing a file"""
    file_url: str
    file_name: str
    mime_type: str
    modification_date: pendulum.DateTime
    size_in_bytes: int
    file_content: Optional[Union[str, bytes]]


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


def filesystem(protocol: str, credentials: FileSystemCredentials = None) -> Tuple[AbstractFileSystem, str]:
    """Instantiates an authenticated fsspec `FileSystem` for a given `protocol` and credentials.

      Please supply credentials instance corresponding to the protocol. The `protocol` is just the code name of the filesystem ie:
      * s3
      * az, abfs
      * gcs, gs

      also see filesystem_from_config
    """
    return filesystem_from_config(FilesystemConfiguration(protocol, credentials))



def filesystem_from_config(config: FilesystemConfiguration) -> Tuple[AbstractFileSystem, str]:
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
    elif proto in ['gcs', 'gs']:
        assert isinstance(config.credentials, GcpCredentials)
        # Default credentials are handled by gcsfs
        if isinstance(config.credentials, CredentialsWithDefault) and config.credentials.has_default_credentials():
            fs_kwargs['token'] = None
        else:
            fs_kwargs['token'] = dict(config.credentials)
        fs_kwargs['project'] = config.credentials.project_id
    try:
        return url_to_fs(config.bucket_url, use_listings_cache=False, **fs_kwargs)  # type: ignore[no-any-return]
    except ModuleNotFoundError as e:
        raise MissingDependencyException("filesystem", [f"{version.DLT_PKG_NAME}[{proto}]"]) from e
