import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.data_writers.escape import (
    escape_athena_identifier,
    format_bigquery_datetime_literal,
)
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration

if t.TYPE_CHECKING:
    from dlt.destinations.impl.athena.athena import AthenaClient


class athena(Destination[AthenaClientConfiguration, "AthenaClient"]):
    spec = AthenaClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        # athena only supports loading from staged files on s3 for now
        caps.preferred_loader_file_format = None
        caps.supported_loader_file_formats = []
        caps.supported_table_formats = ["iceberg"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet", "jsonl"]
        # athena is storing all identifiers in lower case and is case insensitive
        # it also uses lower case in all the queries
        # https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
        caps.escape_identifier = escape_athena_identifier
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = False
        caps.format_datetime_literal = format_bigquery_datetime_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 16 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 262144
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_transactions = False
        caps.alter_add_multi_column = True
        caps.schema_supports_numeric_precision = False
        caps.timestamp_precision = 3
        caps.supports_truncate_command = False
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        return caps

    @property
    def client_class(self) -> t.Type["AthenaClient"]:
        from dlt.destinations.impl.athena.athena import AthenaClient

        return AthenaClient

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
        """Configure the Athena destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            query_result_bucket: S3 bucket to store query results in
            credentials: AWS credentials to connect to the Athena database.
            athena_work_group: Athena work group to use
            aws_data_catalog: Athena data catalog to use
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
