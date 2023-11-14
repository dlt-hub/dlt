import typing as t

from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, Destination

from dlt.destinations.impl.postgres.configuration import PostgresCredentials, PostgresClientConfiguration
from dlt.destinations.impl.postgres import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.postgres.postgres import PostgresClient


class postgres(Destination):

    capabilities = capabilities()
    spec = PostgresClientConfiguration

    @property
    def client_class(self) -> t.Type["PostgresClient"]:
        from dlt.destinations.impl.postgres.postgres import PostgresClient

        return PostgresClient


    @with_config(spec=PostgresClientConfiguration, sections=(known_sections.DESTINATION, 'postgres'), accept_partial=True)
    def __init__(
        self,
        credentials: PostgresCredentials = None,
        create_indexes: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(kwargs['_dlt_config'])
