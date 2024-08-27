import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_databricks_identifier, escape_databricks_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.databricks.configuration import (
    DatabricksCredentials,
    DatabricksClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.databricks.databricks import DatabricksClient


class databricks(Destination[DatabricksClientConfiguration, "DatabricksClient"]):
    spec = DatabricksClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = None
        caps.supported_loader_file_formats = []
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["jsonl", "parquet"]
        caps.escape_identifier = escape_databricks_identifier
        # databricks identifiers are case insensitive and stored in lower case
        # https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
        caps.escape_literal = escape_databricks_literal
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 2 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 16 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supports_truncate_command = True
        # caps.supports_transactions = False
        caps.alter_add_multi_column = True
        caps.supports_multiple_statements = False
        caps.supports_clone_table = True
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        return caps

    @property
    def client_class(self) -> t.Type["DatabricksClient"]:
        from dlt.destinations.impl.databricks.databricks import DatabricksClient

        return DatabricksClient

    def __init__(
        self,
        credentials: t.Union[DatabricksCredentials, t.Dict[str, t.Any], str] = None,
        is_staging_external_location: t.Optional[bool] = False,
        staging_credentials_name: t.Optional[str] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Databricks destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the databricks database. Can be an instance of `DatabricksCredentials` or
                a connection string in the format `databricks://user:password@host:port/database`
            is_staging_external_location: If true, the temporary credentials are not propagated to the COPY command
            staging_credentials_name: If set, credentials with given name will be used in copy command
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            is_staging_external_location=is_staging_external_location,
            staging_credentials_name=staging_credentials_name,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
