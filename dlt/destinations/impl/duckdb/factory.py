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

    def __init__(
        self,
        credentials: t.Union[
            DuckDbCredentials, t.Dict[str, t.Any], str, "DuckDBPyConnection"
        ] = None,
        create_indexes: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the DuckDB destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the duckdb database. Can be an instance of `DuckDbCredentials` or
                a path to a database file. Use :pipeline: to create a duckdb
                in the working folder of the pipeline
            create_indexes: Should unique indexes be created, defaults to False
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
