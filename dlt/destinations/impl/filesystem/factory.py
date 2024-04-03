import typing as t

from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations.impl.filesystem import capabilities
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.storages.configuration import FileSystemCredentials

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

    def __init__(
        self,
        bucket_url: str = None,
        credentials: t.Union[FileSystemCredentials, t.Dict[str, t.Any], t.Any] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the filesystem destination to use in a pipeline and load data to local or remote filesystem.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        The `bucket_url` determines the protocol to be used:

        - Local folder: `file:///path/to/directory`
        - AWS S3 (and S3 compatible storages): `s3://bucket-name
        - Azure Blob Storage: `az://container-name
        - Google Cloud Storage: `gs://bucket-name
        - Memory fs: `memory://m`

        Args:
            bucket_url: The fsspec compatible bucket url to use for the destination.
            credentials: Credentials to connect to the filesystem. The type of credentials should correspond to
                the bucket protocol. For example, for AWS S3, the credentials should be an instance of `AwsCredentials`.
                A dictionary with the credentials parameters can also be provided.
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            bucket_url=bucket_url,
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
