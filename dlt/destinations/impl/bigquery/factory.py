import typing as t

from dlt.common.normalizers.naming import NamingConvention
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_hive_identifier, format_bigquery_datetime_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration

if t.TYPE_CHECKING:
    from dlt.destinations.impl.bigquery.bigquery import BigQueryClient


# noinspection PyPep8Naming
class bigquery(Destination[BigQueryClientConfiguration, "BigQueryClient"]):
    spec = BigQueryClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl", "parquet"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet", "jsonl"]
        # BigQuery is by default case sensitive but that cannot be turned off for a dataset
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
        caps.escape_identifier = escape_hive_identifier
        caps.escape_literal = None
        caps.has_case_sensitive_identifiers = True
        caps.casefold_identifier = str
        # BQ limit is 4GB but leave a large headroom since buffered writer does not preemptively check size
        caps.recommended_file_size = int(1024 * 1024 * 1024)
        caps.format_datetime_literal = format_bigquery_datetime_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (76, 38)
        caps.max_identifier_length = 1024
        caps.max_column_identifier_length = 300
        caps.max_query_length = 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 10 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_clone_table = True
        caps.schema_supports_numeric_precision = False  # no precision information in BigQuery
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]

        return caps

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
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.should_set_case_sensitivity_on_new_dataset:
            caps.has_case_sensitive_identifiers = config.has_case_sensitive_identifiers
        return super().adjust_capabilities(caps, config, naming)
