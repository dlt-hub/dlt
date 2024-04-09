import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)


if t.TYPE_CHECKING:
    from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
    from clickhouse_driver.dbapi import Connection  # type: ignore[import-untyped]


class clickhouse(Destination[ClickHouseClientConfiguration, "ClickHouseClient"]):
    spec = ClickHouseClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["ClickHouseClient"]:
        from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient

        return ClickHouseClient

    def __init__(
        self,
        credentials: t.Union[
            ClickHouseCredentials, str, t.Dict[str, t.Any], t.Type["Connection"]
        ] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the ClickHouse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment
        variables and dlt config files.

        Args:
            credentials: Credentials to connect to the clickhouse database.
                Can be an instance of `ClickHouseCredentials`, or a connection string
                in the format `clickhouse://user:password@host:port/database`.
            **kwargs: Additional arguments passed to the destination config.
        """
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
