import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.qdrant.configuration import QdrantCredentials, QdrantClientConfiguration
from dlt.destinations.impl.qdrant import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.qdrant.qdrant_client import QdrantClient


class qdrant(Destination[QdrantClientConfiguration, "QdrantClient"]):
    spec = QdrantClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["QdrantClient"]:
        from dlt.destinations.impl.qdrant.qdrant_client import QdrantClient

        return QdrantClient

    def __init__(
        self,
        credentials: t.Union[QdrantCredentials, t.Dict[str, t.Any]] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
