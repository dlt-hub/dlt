import typing as t

from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, Destination

from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration
from dlt.destinations.impl.duckdb import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.duckdb.duck import DuckDbClient


class duckdb(Destination):

    capabilities = capabilities()
    spec = DuckDbClientConfiguration

    @property
    def client_class(self) -> t.Type["DuckDbClient"]:
        from dlt.destinations.impl.duckdb.duck import DuckDbClient

        return DuckDbClient

    @with_config(spec=DuckDbClientConfiguration, sections=(known_sections.DESTINATION, 'duckdb'), accept_partial=True)
    def __init__(
        self,
        credentials: DuckDbCredentials = None,
        create_indexes: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(kwargs['_dlt_config'])
