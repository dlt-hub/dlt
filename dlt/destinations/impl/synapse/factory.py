import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_mssql_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.impl.synapse.configuration import (
    SynapseCredentials,
    SynapseClientConfiguration,
)
from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType

if t.TYPE_CHECKING:
    from dlt.destinations.impl.synapse.synapse import SynapseClient


class synapse(Destination[SynapseClientConfiguration, "SynapseClient"]):
    spec = SynapseClientConfiguration

    # TODO: implement as property everywhere and makes sure not accessed as class property
    # @property
    # def spec(self) -> t.Type[SynapseClientConfiguration]:
    #     return SynapseClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()

        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet"]

        caps.insert_values_writer_type = "select_union"  # https://stackoverflow.com/a/77014299

        # similarly to mssql case sensitivity depends on database collation
        # https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support?view=sql-server-ver16#collations-in-azure-sql-database
        # note that special option CATALOG_COLLATION is used to change it
        caps.escape_identifier = escape_postgres_identifier
        caps.escape_literal = escape_mssql_literal
        # we allow to reconfigure capabilities in the mssql factory
        caps.has_case_sensitive_identifiers = False

        # Synapse has a max precision of 38
        # https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#DataTypes
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)

        # https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#LimitationsRestrictions
        caps.max_identifier_length = 128
        caps.max_column_identifier_length = 128

        # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-service-capacity-limits#queries
        caps.max_query_length = 65536 * 4096
        caps.is_max_query_length_in_bytes = True

        # nvarchar(max) can store 2 GB
        # https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql?view=sql-server-ver16#nvarchar---n--max--
        caps.max_text_data_type_length = 2 * 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True

        # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-develop-transactions
        caps.supports_transactions = True
        caps.supports_ddl_transactions = False

        caps.supports_create_table_if_not_exists = (
            False  # IF NOT EXISTS on CREATE TABLE not supported
        )

        # Synapse throws "Some part of your SQL statement is nested too deeply. Rewrite the query or break it up into smaller queries."
        # if number of records exceeds a certain number. Which exact number that is seems not deterministic:
        # in tests, I've seen a query with 12230 records run succesfully on one run, but fail on a subsequent run, while the query remained exactly the same.
        # 10.000 records is a "safe" amount that always seems to work.
        caps.max_rows_per_insert = 10000

        # datetimeoffset can store 7 digits for fractional seconds
        # https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-ver16
        caps.timestamp_precision = 7

        caps.supported_merge_strategies = ["delete-insert", "scd2"]

        return caps

    @property
    def client_class(self) -> t.Type["SynapseClient"]:
        from dlt.destinations.impl.synapse.synapse import SynapseClient

        return SynapseClient

    def __init__(
        self,
        credentials: t.Union[SynapseCredentials, t.Dict[str, t.Any], str] = None,
        default_table_index_type: t.Optional[TTableIndexType] = "heap",
        create_indexes: bool = False,
        staging_use_msi: bool = False,
        has_case_sensitive_identifiers: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Synapse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the Synapse dedicated pool. Can be an instance of `SynapseCredentials` or
                a connection string in the format `synapse://user:password@host:port/database`
            default_table_index_type: Maps directly to the default_table_index_type attribute of the SynapseClientConfiguration object.
            create_indexes: Maps directly to the create_indexes attribute of the SynapseClientConfiguration object.
            staging_use_msi: Maps directly to the staging_use_msi attribute of the SynapseClientConfiguration object.
            has_case_sensitive_identifiers: Are identifiers used by synapse database case sensitive (following the catalog collation)
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            default_table_index_type=default_table_index_type,
            create_indexes=create_indexes,
            staging_use_msi=staging_use_msi,
            has_case_sensitive_identifiers=has_case_sensitive_identifiers,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: SynapseClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)
