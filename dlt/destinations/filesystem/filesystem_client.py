from typing import cast, Tuple

from fsspec.core import url_to_fs
from fsspec import AbstractFileSystem

from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration, GcpServiceAccountCredentials, AwsCredentials


def client_from_config(config: FilesystemClientConfiguration) -> Tuple[AbstractFileSystem, str]:
    proto = config.protocol
    fs_kwargs = {}
    if proto == "s3":
        credentials = cast(AwsCredentials, config.credentials)
        fs_kwargs['key'] = credentials.access_key_id
        fs_kwargs['secret'] = credentials.access_key_secret
        fs_kwargs['token'] = credentials.session_token
        fs_kwargs['profile'] = credentials.profile_name
    elif proto == 'gcs':
        fs_kwargs['token'] = config.credentials.to_native_credentials()
    return url_to_fs(config.bucket_url, **fs_kwargs)  # type: ignore[no-any-return]
