import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseClientConfiguration,
    ClickhouseCredentials,
)


if t.TYPE_CHECKING:
    from dlt.destinations.impl.clickhouse.clickhouse import ClickhouseClient


# noinspection PyPep8Naming
class clickhouse(Destination[ClickhouseClientConfiguration, ClickhouseClient]):
    spec = ClickhouseClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["ClickhouseClient"]:
        from dlt.destinations.impl.clickhouse.clickhouse import ClickhouseClient

        return ClickhouseClient

    def __init__(
        self,
        credentials: ClickhouseCredentials = None,
        dataset_name: str = None,
        default_schema_name: str = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            dataset_name=dataset_name,
            default_schema_name=default_schema_name,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
