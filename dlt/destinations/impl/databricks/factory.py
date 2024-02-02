import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.databricks.configuration import (
    DatabricksCredentials,
    DatabricksClientConfiguration,
)
from dlt.destinations.impl.databricks import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.databricks.databricks import DatabricksClient


class databricks(Destination[DatabricksClientConfiguration, "DatabricksClient"]):
    spec = DatabricksClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["DatabricksClient"]:
        from dlt.destinations.impl.databricks.databricks import DatabricksClient

        return DatabricksClient

    def __init__(
        self,
        credentials: t.Union[DatabricksCredentials, t.Dict[str, t.Any], str] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Databricks destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the databricks database. Can be an instance of `DatabricksCredentials` or
                a connection string in the format `databricks://user:password@host:port/database`
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
