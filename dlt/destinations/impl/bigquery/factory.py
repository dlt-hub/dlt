import typing as t

from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.destinations.impl.bigquery import capabilities
from dlt.common.destination import Destination, DestinationCapabilitiesContext

if t.TYPE_CHECKING:
    from dlt.destinations.impl.bigquery.bigquery import BigQueryClient


# noinspection PyPep8Naming
class bigquery(Destination[BigQueryClientConfiguration, "BigQueryClient"]):
    spec = BigQueryClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["BigQueryClient"]:
        from dlt.destinations.impl.bigquery.bigquery import BigQueryClient

        return BigQueryClient

    def __init__(
        self,
        credentials: t.Optional[GcpServiceAccountCredentials] = None,
        location: t.Optional[str] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            location=location,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
