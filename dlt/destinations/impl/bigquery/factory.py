import typing as t

from dlt.common.normalizers.naming import NamingConvention
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
        has_case_sensitive_identifiers: bool = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the MsSql destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the mssql database. Can be an instance of `GcpServiceAccountCredentials` or
                a dict or string with service accounts credentials as used in the Google Cloud
            location: A location where the datasets will be created, eg. "EU". The default is "US"
            has_case_sensitive_identifiers: Is the dataset case-sensitive, defaults to True
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            location=location,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: BigQueryClientConfiguration,
        naming: NamingConvention,
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        caps.has_case_sensitive_identifiers = config.has_case_sensitive_identifiers
        return super().adjust_capabilities(caps, config, naming)
