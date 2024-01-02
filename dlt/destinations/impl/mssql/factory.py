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
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the MsSql destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the mssql database. Can be an instance of `MsSqlCredentials` or
                a connection string in the format `mssql://user:password@host:port/database`
            create_indexes: Should unique indexes be created
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
