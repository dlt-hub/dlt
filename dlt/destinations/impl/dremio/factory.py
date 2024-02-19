import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.dremio.configuration import DremioClientConfiguration
from dlt.common.configuration.specs import AwsCredentials
from dlt.destinations.impl.dremio import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.dremio.dremio import DremioClient


class athena(Destination[DremioClientConfiguration, "DremioClient"]):
    spec = DremioClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["DremioClient"]:
        from dlt.destinations.impl.dremio.dremio import DremioClient

        return DremioClient

    def __init__(
        self,
        query_result_bucket: t.Optional[str] = None,
        credentials: t.Union[AwsCredentials, t.Dict[str, t.Any], t.Any] = None,
        athena_work_group: t.Optional[str] = None,
        aws_data_catalog: t.Optional[str] = "awsdatacatalog",
        force_iceberg: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Dremio destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            query_result_bucket: S3 bucket to store query results in
            credentials: AWS credentials to connect to the Dremio database.
            athena_work_group: Dremio work group to use
            aws_data_catalog: Dremio data catalog to use
            force_iceberg: Force iceberg tables
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            query_result_bucket=query_result_bucket,
            credentials=credentials,
            athena_work_group=athena_work_group,
            aws_data_catalog=aws_data_catalog,
            force_iceberg=force_iceberg,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
