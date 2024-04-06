import typing as t

from clickhouse_driver.dbapi import Connection  # type: ignore[import-untyped]

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseClientConfiguration,
    ClickhouseCredentials,
)


if t.TYPE_CHECKING:
    from dlt.destinations.impl.clickhouse.clickhouse import ClickhouseClient


# noinspection PyPep8Naming
class clickhouse(Destination[ClickhouseClientConfiguration, "ClickhouseClient"]):
    spec = ClickhouseClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["ClickhouseClient"]:
        from dlt.destinations.impl.clickhouse.clickhouse import ClickhouseClient

        return ClickhouseClient

    def __init__(
        self,
        credentials: t.Union[ClickhouseCredentials, str, t.Dict[str, t.Any], Connection] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Clickhouse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment
        variables and dlt config files.

        Args:
            credentials: Credentials to connect to the clickhouse database.
                Can be an instance of `ClickhouseCredentials`, or a connection string
                in the format `clickhouse://user:password@host:port/database`.
            **kwargs: Additional arguments passed to the destination config.
        """
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
