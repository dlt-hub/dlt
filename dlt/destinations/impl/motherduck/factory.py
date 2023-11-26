import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckCredentials,
    MotherDuckClientConfiguration,
)
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
        credentials: t.Union[
            MotherDuckCredentials, str, t.Dict[str, t.Any], "DuckDBPyConnection"
        ] = None,
        create_indexes: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the MotherDuck destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the MotherDuck database. Can be an instance of `MotherDuckCredentials` or
                a connection string in the format `md:///<database_name>?token=<service token>`
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
