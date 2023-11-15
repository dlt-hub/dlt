import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, MsSqlClientConfiguration
from dlt.destinations.impl.mssql import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.mssql.mssql import MsSqlClient


class mssql(Destination[MsSqlClientConfiguration, "MsSqlClient"]):

    spec = MsSqlClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["MsSqlClient"]:
        from dlt.destinations.impl.mssql.mssql import MsSqlClient

        return MsSqlClient

    def __init__(
        self,
        credentials: t.Union[MsSqlCredentials, t.Dict[str, t.Any], str] = None,
        create_indexes: bool = True,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(credentials=credentials, create_indexes=create_indexes, **kwargs)
