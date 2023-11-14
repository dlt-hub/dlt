import typing as t

from dlt.common.configuration import with_config, known_sections
from dlt.common.destination.reference import DestinationClientConfiguration, Destination

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, MsSqlClientConfiguration
from dlt.destinations.impl.mssql import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.mssql.mssql import MsSqlClient


class mssql(Destination):

    capabilities = capabilities()
    spec = MsSqlClientConfiguration

    @property
    def client_class(self) -> t.Type["MsSqlClient"]:
        from dlt.destinations.impl.mssql.mssql import MsSqlClient

        return MsSqlClient

    def __init__(
        self,
        credentials: t.Union[MsSqlCredentials, str] = None,
        create_indexes: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, create_indexes=create_indexes, **kwargs)
