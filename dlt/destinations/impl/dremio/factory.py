import typing as t

from dlt.destinations.impl.dremio.configuration import (
    DremioCredentials,
    DremioClientConfiguration,
)
from dlt.destinations.impl.dremio import capabilities
from dlt.common.destination import Destination, DestinationCapabilitiesContext

if t.TYPE_CHECKING:
    from dlt.destinations.impl.dremio.dremio import DremioClient


class dremio(Destination[DremioClientConfiguration, "DremioClient"]):
    spec = DremioClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["DremioClient"]:
        from dlt.destinations.impl.dremio.dremio import DremioClient

        return DremioClient

    def __init__(
        self,
        credentials: t.Union[DremioCredentials, t.Dict[str, t.Any], str] = None,
        staging_data_source: str = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Dremio destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the dremio database. Can be an instance of `DremioCredentials` or
                a connection string in the format `dremio://user:password@host:port/database`
            staging_data_source: The name of the "Object Storage" data source in Dremio containing the s3 bucket
        """
        super().__init__(
            credentials=credentials,
            staging_data_source=staging_data_source,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
