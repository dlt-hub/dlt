import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.sink.configuration import (
    SinkClientConfiguration,
    SinkClientCredentials,
)
from dlt.destinations.impl.sink import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.sink.sink import SinkClient


class sink(Destination[SinkClientConfiguration, "SinkClient"]):
    spec = SinkClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["SinkClient"]:
        from dlt.destinations.impl.sink.sink import SinkClient

        return SinkClient

    def __init__(
        self,
        credentials: SinkClientCredentials = None,
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
