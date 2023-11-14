import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.motherduck.configuration import MotherDuckCredentials, MotherDuckClientConfiguration
from dlt.destinations.impl.motherduck import capabilities

if t.TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.motherduck.motherduck import MotherDuckClient


class motherduck(Destination[MotherDuckClientConfiguration, "MotherDuckClient"]):

    spec = MotherDuckClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["MotherDuckClient"]:
        from dlt.destinations.impl.motherduck.motherduck import MotherDuckClient

        return MotherDuckClient

    def __init__(
        self,
        credentials: t.Union[MotherDuckCredentials, str, "DuckDBPyConnection"] = None,
        create_indexes: bool = False,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, create_indexes=create_indexes, **kwargs)
