import typing as t

from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.dummy.configuration import DummyClientConfiguration, DummyClientCredentials
from dlt.destinations.impl.dummy import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.dummy.dummy import DummyClient


class dummy(Destination):

    spec = DummyClientConfiguration

    @property
    def capabilitites(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["DummyClient"]:
        from dlt.destinations.impl.dummy.dummy import DummyClient

        return DummyClient

    @with_config(spec=DummyClientConfiguration, sections=(known_sections.DESTINATION, 'dummy'), accept_partial=True)
    def __init__(
        self,
        credentials: DummyClientCredentials = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(kwargs['_dlt_config'])
