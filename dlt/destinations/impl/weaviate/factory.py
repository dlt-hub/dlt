import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.weaviate.configuration import WeaviateCredentials, WeaviateClientConfiguration
from dlt.destinations.impl.weaviate import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient


class weaviate(Destination[WeaviateClientConfiguration, "WeaviateClient"]):

    spec = WeaviateClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["WeaviateClient"]:
        from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient

        return WeaviateClient

    def __init__(
        self,
        credentials: t.Optional[WeaviateCredentials] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, **kwargs)
