import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration
from dlt.destinations.impl.duckdb import capabilities

if t.TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.duckdb.duck import DuckDbClient


class duckdb(Destination[DuckDbClientConfiguration, "DuckDbClient"]):

    spec = DuckDbClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["DuckDbClient"]:
        from dlt.destinations.impl.duckdb.duck import DuckDbClient

        return DuckDbClient

    # @with_config(spec=DuckDbClientConfiguration, sections=(known_sections.DESTINATION, 'duckdb'), accept_partial=True)
    def __init__(
        self,
        credentials: t.Union[DuckDbCredentials, str, "DuckDBPyConnection"] = None,
        create_indexes: bool = False,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, create_indexes=create_indexes, **kwargs)
