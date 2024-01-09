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
        stage_name: t.Optional[str] = None,
        keep_staged_files: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Databricks destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the databricks database. Can be an instance of `DatabricksCredentials` or
                a connection string in the format `databricks://user:password@host:port/database`
            stage_name: Name of the stage to use for staging files. If not provided, the default stage will be used.
            keep_staged_files: Should staged files be kept after loading. If False, staged files will be deleted after loading.
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            stage_name=stage_name,
            keep_staged_files=keep_staged_files,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
