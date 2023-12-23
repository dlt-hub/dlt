import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.weaviate.configuration import (
    WeaviateCredentials,
    WeaviateClientConfiguration,
)
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
        credentials: t.Union[WeaviateCredentials, t.Dict[str, t.Any]] = None,
        vectorizer: str = None,
        module_config: t.Dict[str, t.Dict[str, str]] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Weaviate destination to use in a pipeline.

        All destination config parameters can be provided as arguments here and will supersede other config sources (such as dlt config files and environment variables).

        Args:
            credentials: Weaviate credentials containing URL, API key and optional headers
            vectorizer: The name of the Weaviate vectorizer to use
            module_config: The configuration for the Weaviate modules
            **kwargs: Additional arguments forwarded to the destination config
        """
        super().__init__(
            credentials=credentials,
            vectorizer=vectorizer,
            module_config=module_config,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
