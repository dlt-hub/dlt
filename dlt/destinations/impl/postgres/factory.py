import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)
from dlt.destinations.impl.postgres import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.postgres.postgres import PostgresClient


class postgres(Destination[PostgresClientConfiguration, "PostgresClient"]):
    spec = PostgresClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["PostgresClient"]:
        from dlt.destinations.impl.postgres.postgres import PostgresClient

        return PostgresClient

    def __init__(
        self,
        credentials: t.Union[PostgresCredentials, t.Dict[str, t.Any], str] = None,
        create_indexes: bool = True,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Postgres destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the postgres database. Can be an instance of `PostgresCredentials` or
                a connection string in the format `postgres://user:password@host:port/database`
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
