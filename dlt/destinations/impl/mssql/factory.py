from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.naming.naming import NamingConvention
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_mssql_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.mssql.configuration import MsSqlCredentials, MsSqlClientConfiguration

if TYPE_CHECKING:
    from dlt.destinations.impl.mssql.mssql import MsSqlJobClient


class MsSqlTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "json",
        "text": "nvarchar(max)",
        "double": "float",
        "bool": "bit",
        "bigint": "bigint",
        "binary": "varbinary(max)",
        "date": "date",
        "timestamp": "datetimeoffset",
        "time": "time",
    }

    sct_to_dbt = {
        "text": "nvarchar(%i)",
        "timestamp": "datetimeoffset(%i)",
        "binary": "varbinary(%i)",
        "decimal": "decimal(%i,%i)",
        "time": "time(%i)",
        "wei": "decimal(%i,%i)",
    }

    dbt_to_sct = {
        "nvarchar": "text",
        "float": "double",
        "bit": "bool",
        "datetimeoffset": "timestamp",
        "date": "date",
        "bigint": "bigint",
        "varbinary": "binary",
        "decimal": "decimal",
        "time": "time",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
        "json": "json",
    }

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "bigint"
        if precision <= 8:
            return "tinyint"
        if precision <= 16:
            return "smallint"
        if precision <= 32:
            return "int"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with `{precision=:}` can't be mapped to MSSQL integer type"
        )

    def from_destination_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        if db_type == "decimal":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
        return super().from_destination_type(db_type, precision, scale)


class mssql(Destination[MsSqlClientConfiguration, "MsSqlJobClient"]):
    spec = MsSqlClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "model"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.type_mapper = MsSqlTypeMapper
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
        caps.supports_multiple_statements = True
        caps.supports_create_table_if_not_exists = False  # IF NOT EXISTS not supported
        caps.max_rows_per_insert = 1000
        caps.timestamp_precision = 7
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.sqlglot_dialect = "tsql"

        return caps

    @property
    def client_class(self) -> Type["MsSqlJobClient"]:
        from dlt.destinations.impl.mssql.mssql import MsSqlJobClient

        return MsSqlJobClient

    def __init__(
        self,
        credentials: Union[MsSqlCredentials, Dict[str, Any], str] = None,
        create_indexes: bool = False,
        has_case_sensitive_identifiers: bool = False,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the MsSql destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[MsSqlCredentials, Dict[str, Any], str], optional): Credentials to connect to the mssql database. Can be an instance of `MsSqlCredentials` or
                a connection string in the format `mssql://user:password@host:port/database`
            create_indexes (bool, optional): Should unique indexes be created
            has_case_sensitive_identifiers (bool, optional): Are identifiers used by mssql database case sensitive (following the collation)
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
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
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # modify the caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)


mssql.register()
