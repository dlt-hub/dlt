import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming.naming import NamingConvention
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_mssql_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, MsSqlClientConfiguration

if t.TYPE_CHECKING:
    from dlt.destinations.impl.mssql.mssql import MsSqlJobClient


class mssql(Destination[MsSqlClientConfiguration, "MsSqlJobClient"]):
    spec = MsSqlClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        # mssql is by default case insensitive and stores identifiers as is
        # case sensitivity can be changed by database collation so we allow to reconfigure
        # capabilities in the mssql factory
        caps.escape_identifier = escape_postgres_identifier
        caps.escape_literal = escape_mssql_literal
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        # https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-ver16&redirectedfrom=MSDN
        caps.max_identifier_length = 128
        caps.max_column_identifier_length = 128
        # A SQL Query can be a varchar(max) but is shown as limited to 65,536 * Network Packet
        caps.max_query_length = 65536 * 10
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 2**30 - 1
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = True
        caps.supports_create_table_if_not_exists = False  # IF NOT EXISTS not supported
        caps.max_rows_per_insert = 1000
        caps.timestamp_precision = 7
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]

        return caps

    @property
    def client_class(self) -> t.Type["MsSqlJobClient"]:
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        return MsSqlJobClient

    def __init__(
        self,
        credentials: t.Union[MsSqlCredentials, t.Dict[str, t.Any], str] = None,
        create_indexes: bool = False,
        has_case_sensitive_identifiers: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the MsSql destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the mssql database. Can be an instance of `MsSqlCredentials` or
                a connection string in the format `mssql://user:password@host:port/database`
            create_indexes: Should unique indexes be created
            has_case_sensitive_identifiers: Are identifiers used by mssql database case sensitive (following the collation)
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: MsSqlClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)
