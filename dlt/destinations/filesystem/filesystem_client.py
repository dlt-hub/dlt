from typing import cast, Tuple

from fsspec.core import url_to_fs
from fsspec import AbstractFileSystem

from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration, GcpServiceAccountCredentials, AwsCredentials
from dlt import version


def client_from_config(config: FilesystemClientConfiguration) -> Tuple[AbstractFileSystem, str]:
    proto = config.protocol
    fs_kwargs = {}
    if proto == "s3":
        credentials = cast(AwsCredentials, config.credentials)
        fs_kwargs.update(credentials.to_s3fs_credentials())
    elif proto == 'gcs':
        fs_kwargs['token'] = config.credentials.to_native_credentials()
    try:
        return url_to_fs(config.bucket_url, **fs_kwargs)  # type: ignore[no-any-return]
    except ImportError as e:
        raise MissingDependencyException("filesystem destination", [f"{version.DLT_PKG_NAME}[{proto}]"]) from e
