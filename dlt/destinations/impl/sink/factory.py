import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.sink.configuration import (
    SinkClientConfiguration,
    SinkClientCredentials,
    TSinkCallable,
)
from dlt.destinations.impl.sink import capabilities
from dlt.common.data_writers import TLoaderFileFormat

if t.TYPE_CHECKING:
    from dlt.destinations.impl.sink.sink import SinkClient


class sink(Destination[SinkClientConfiguration, "SinkClient"]):
    spec = SinkClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities(
            self.config_params.get("loader_file_format", "puae-jsonl"),
            self.config_params.get("naming_convention", "direct"),
        )

    @property
    def client_class(self) -> t.Type["SinkClient"]:
        from dlt.destinations.impl.sink.sink import SinkClient

        return SinkClient

    def __init__(
        self,
        credentials: t.Union[SinkClientCredentials, TSinkCallable] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        loader_file_format: TLoaderFileFormat = None,
        batch_size: int = 10,
        naming_convention: str = "direct",
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            loader_file_format=loader_file_format,
            batch_size=batch_size,
            naming_convention=naming_convention,
            **kwargs,
        )
