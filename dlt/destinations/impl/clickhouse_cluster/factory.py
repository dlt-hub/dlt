from typing import TYPE_CHECKING, Any, Dict, Type, Union

from dlt.common.destination import Destination
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse.factory import clickhouse as clickhouse_factory
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
    ClickHouseClusterCredentials,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
    from clickhouse_driver.dbapi import Connection  # type: ignore[import-untyped]
else:
    Connection = Any


class clickhouse_cluster(
    Destination[ClickHouseClusterClientConfiguration, "ClickHouseClusterClient"]
):
    spec = ClickHouseClusterClientConfiguration

    @property
    def client_class(self) -> type["ClickHouseClusterClient"]:
        from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import (
            ClickHouseClusterClient,
        )

        return ClickHouseClusterClient

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps = clickhouse_factory._set_raw_capabilities(caps)
        return caps

    def __init__(
        self,
        credentials: Union[
            ClickHouseClusterCredentials, str, Dict[str, Any], Type[Connection]
        ] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the ClickHouse Cluster destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment
        variables and dlt config files.

        Args:
            credentials (Union[ClickHouseClusterCredentials, str, Dict[str, Any], Type[Connection]], optional):
                Credentials to connect to the clickhouse cluster database. Can be an instance of
                `ClickHouseClusterCredentials` or a connection string in the format
                `clickhouse://user:password@host:port/database`
            destination_name (str, optional): Name of the destination, can be used in config section
                to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


clickhouse_cluster.register()
