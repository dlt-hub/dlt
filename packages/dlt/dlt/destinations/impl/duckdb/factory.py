from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING, Sequence, Tuple

from dlt.common import logger
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_duckdb_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.pipeline import SupportsPipeline
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials, DuckDbClientConfiguration

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.duckdb.duck import DuckDbClient
else:
    DuckDBPyConnection = Any


class DuckDbTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "JSON",
        "text": "VARCHAR",
        "double": "DOUBLE",
        "bool": "BOOLEAN",
        "date": "DATE",
        # Duck does not allow specifying precision on timestamp with tz
        "timestamp": "TIMESTAMP WITH TIME ZONE",
        "bigint": "BIGINT",
        "binary": "BLOB",
        "time": "TIME",
    }

    sct_to_dbt = {
        # VARCHAR(n) is alias for VARCHAR in duckdb
        # "text": "VARCHAR(%i)",
        "decimal": "DECIMAL(%i,%i)",
        "wei": "DECIMAL(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "JSON": "json",
        "DOUBLE": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP WITH TIME ZONE": "timestamp",
        "BLOB": "binary",
        "DECIMAL": "decimal",
        "TIME": "time",
        # Int types
        "TINYINT": "bigint",
        "SMALLINT": "bigint",
        "INTEGER": "bigint",
        "BIGINT": "bigint",
        "HUGEINT": "bigint",
        "TIMESTAMP_S": "timestamp",
        "TIMESTAMP_MS": "timestamp",
        "TIMESTAMP_NS": "timestamp",
    }

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "BIGINT"
        # Precision is number of bits
        if precision <= 8:
            return "TINYINT"
        elif precision <= 16:
            return "SMALLINT"
        elif precision <= 32:
            return "INTEGER"
        elif precision <= 64:
            return "BIGINT"
        elif precision <= 128:
            return "HUGEINT"
        raise TerminalValueError(
            f"bigint with `{precision=:}` can't be mapped to DuckDB integer type"
        )

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        column_name = column["name"]
        table_name = table["name"]
        timezone = column.get("timezone", True)
        precision = column.get("precision")

        if timezone and precision is not None and precision != 6:
            logger.warn(
                f"DuckDB does not support both timezone and precision for column '{column_name}' in"
                f" table '{table_name}'. Will default to timezone. Please set timezone to False to"
                " use precision types."
            )

        if timezone:
            # default timestamp mapping for timezone
            return None

        if precision is None or precision == 6:
            return "TIMESTAMP"
        elif precision == 0:
            return "TIMESTAMP_S"
        elif precision == 3:
            return "TIMESTAMP_MS"
        elif precision == 9:
            return "TIMESTAMP_NS"

        raise TerminalValueError(
            f"DuckDB doesn't support `{precision=:}` for datetime column `{column_name}` in table"
            f" `{table_name}`"
        )

    def from_destination_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        # duckdb provides the types with scale and precision
        db_type = db_type.split("(")[0].upper()
        if db_type == "DECIMAL":
            if precision == 38 and scale == 0:
                return dict(data_type="wei", precision=precision, scale=scale)
        return super().from_destination_type(db_type, precision, scale)


class duckdb(Destination[DuckDbClientConfiguration, "DuckDbClient"]):
    spec = DuckDbClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "parquet", "jsonl", "model"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.type_mapper = DuckDbTypeMapper
        caps.escape_identifier = escape_postgres_identifier
        # all identifiers are case insensitive but are stored as is
        caps.escape_literal = escape_duckdb_literal
        caps.has_case_sensitive_identifiers = False
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 65536
        caps.max_column_identifier_length = 65536
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.alter_add_multi_column = False
        caps.supports_truncate_command = False
        caps.supported_merge_strategies = ["delete-insert", "scd2"]
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.sqlglot_dialect = "duckdb"

        return caps

    @property
    def client_class(self) -> Type["DuckDbClient"]:
        from dlt.destinations.impl.duckdb.duck import DuckDbClient

        return DuckDbClient

    def __init__(
        self,
        credentials: Union[DuckDbCredentials, Dict[str, Any], str, DuckDBPyConnection] = None,
        create_indexes: bool = False,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the DuckDB destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[DuckDbCredentials, Dict[str, Any], str, DuckDBPyConnection], optional): Credentials to connect to the duckdb database. Can be an instance of `DuckDbCredentials` or
                a path to a database file. Use :pipeline: to create a duckdb in the working folder of the pipeline.
                Instance of `DuckDbCredentials` allows to pass extensions, configs and pragmas to be set up for connection.
            create_indexes (bool, optional): Should unique indexes be created, defaults to False
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """

        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


duckdb.register()
