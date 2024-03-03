import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.filesystem import capabilities
from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration


if t.TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


class filesystem(Destination[FilesystemDestinationClientConfiguration, "FilesystemClient"]):
    spec = FilesystemDestinationClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["FilesystemClient"]:
        from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

        return FilesystemClient

    def __init__(self, bucket_url: str = None, credentials: t.Union[FileSystemCredentials, t.Dict[str, t.Any], t.Any] = None,
                 destination_name: t.Optional[str] = None, environment: t.Optional[str] = None, layout_placeholders: t.Optional[DictStrAny] = None, **kwargs: t.Any) -> None:
        """Configure the filesystem destination to use in a pipeline and load data to local or remote filesystem.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        The `bucket_url` determines the protocol to be used:

        - Local folder: `file:///path/to/directory`
        - AWS S3 (and S3 compatible storages): `s3://bucket-name`
        - Azure Blob Storage: `az://container-name`
        - Google Cloud Storage: `gs://bucket-name`
        - Memory fs: `memory://m`

        Args:
            bucket_url: The fsspec compatible bucket url to use for the destination.
            credentials: Credentials to connect to the filesystem. The type of credentials should correspond to
                the bucket protocol. For example, for AWS S3, the credentials should be an instance of `AwsCredentials`.
                A dictionary with the credentials parameters can be provided.
            layout_placeholders: A dictionary to define custom placeholders for partition layouts.
                Keys are placeholder strings and values can be literals or functions returning strings.
                Useful for customizing destination partitioning paths based on source properties, metadata, or user-defined functions.
            **kwargs: Additional arguments passed to the destination config.
        """
        if layout_placeholders is None:
            layout_placeholders = {}
        super().__init__(
            bucket_url=bucket_url,
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            layout_placeholders=layout_placeholders,
            **kwargs,
        )
