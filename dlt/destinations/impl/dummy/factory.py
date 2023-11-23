import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.dummy.configuration import (
    DummyClientConfiguration,
    DummyClientCredentials,
)
from dlt.destinations.impl.dummy import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.dummy.dummy import DummyClient


class dummy(Destination[DummyClientConfiguration, "DummyClient"]):
    spec = DummyClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["DummyClient"]:
        from dlt.destinations.impl.dummy.dummy import DummyClient

        return DummyClient

    def __init__(
        self,
        credentials: DummyClientCredentials = None,
        name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, name=name, environment=environment, **kwargs)
