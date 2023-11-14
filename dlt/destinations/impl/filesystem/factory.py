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
        credentials: FileSystemCredentials = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(bucket_url=bucket_url, credentials=credentials, **kwargs)
