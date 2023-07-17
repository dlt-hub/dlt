from typing import Tuple, cast

from fsspec import AbstractFileSystem
from fsspec.core import url_to_fs

from dlt import version
from dlt.common.configuration.specs import CredentialsWithDefault
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny
from dlt.destinations.filesystem.configuration import (
    AwsCredentials,
    FilesystemClientConfiguration,
    GcpCredentials,
    GcpServiceAccountCredentials,
)


def client_from_config(config: FilesystemClientConfiguration) -> Tuple[AbstractFileSystem, str]:
    proto = config.protocol
    fs_kwargs: DictStrAny = {}
    if proto == "s3":
        credentials = cast(AwsCredentials, config.credentials)
        fs_kwargs.update(credentials.to_s3fs_credentials())
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
        return url_to_fs(config.bucket_url, **fs_kwargs)  # type: ignore[no-any-return]
    except ImportError as e:
        raise MissingDependencyException(
            "filesystem destination", [f"{version.DLT_PKG_NAME}[{proto}]"]
        ) from e
