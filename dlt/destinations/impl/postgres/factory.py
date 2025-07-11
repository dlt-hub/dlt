from typing import Any, Optional, Sequence, Tuple, Type, Union, Dict, TYPE_CHECKING

from dlt.common import logger
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnSchema, TColumnType, TTableSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)
from dlt.destinations.impl.postgres.postgres_adapter import GEOMETRY_HINT, SRID_HINT
from dlt.destinations.type_mapping import TypeMapperImpl

if TYPE_CHECKING:
    from dlt.destinations.impl.postgres.postgres import PostgresClient


class PostgresTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "json": "jsonb",
        "text": "varchar",
        "double": "double precision",
        "bool": "boolean",
        "date": "date",
        "bigint": "bigint",
        "binary": "bytea",
        "timestamp": "timestamp with time zone",
        "time": "time without time zone",
    }

    sct_to_dbt = {
        "text": "varchar(%i)",
        "timestamp": "timestamp (%i) with time zone",
        "decimal": "numeric(%i,%i)",
        "time": "time (%i) without time zone",
        "wei": "numeric(%i,%i)",
    }

    dbt_to_sct = {
        "varchar": "text",
        "jsonb": "json",
        "double precision": "double",
        "boolean": "bool",
        "timestamp with time zone": "timestamp",
        "timestamp without time zone": "timestamp",
        "date": "date",
        "bigint": "bigint",
        "bytea": "binary",
        "numeric": "decimal",
        "time without time zone": "time",
        "character varying": "text",
        "smallint": "bigint",
        "integer": "bigint",
        "geometry": "text",
    }

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        precision = column.get("precision")
        if precision is None:
            return "bigint"
        # Precision is number of bits
        if precision <= 16:
            return "smallint"
        elif precision <= 32:
            return "integer"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with `{precision=:}` can't be mapped to PostgreSQL integer type"
        )

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        column_name = column.get("name")
        table_name = table.get("name")
        timezone = column.get("timezone")
        precision = column.get("precision")

        if timezone is None and precision is None:
            return None

        timestamp = "timestamp"

        # append precision if specified and valid
        if precision is not None:
            if 0 <= precision <= 6:
                timestamp += f" ({precision})"
            else:
                raise TerminalValueError(
                    f"Postgres doesn't support {precision=:} for timestamp column `{column_name}`"
                    f" in table `{table_name}`"
                )

        # append timezone part
        if timezone is None or timezone:  # timezone True and None
            timestamp += " with time zone"
        else:  # timezone is explicitly False
            timestamp += " without time zone"

        return timestamp

    def from_destination_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "numeric" and (precision, scale) == self.capabilities.wei_precision:
            return dict(data_type="wei")
        if db_type.startswith("geometry"):
            return dict(data_type="text")
        if db_type.startswith("json"):
            return dict(data_type="json")
        return super().from_destination_type(db_type, precision, scale)

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        if column.get(GEOMETRY_HINT):
            srid = column.get(SRID_HINT, 4326)
            return f"geometry(Geometry, {srid})"
        if column["data_type"] == "json" and table.get("file_format") == "parquet":
            return "json"  # adbc is not able to copy into JSONB columns
        return super().to_destination_type(column, table)


def postgres_loader_file_format_selector(
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    try:
        # supports adbc for direct parquet loading
        import adbc_driver_postgresql.dbapi
    except ImportError:
        supported_loader_file_formats = list(supported_loader_file_formats)
        supported_loader_file_formats.remove("parquet")

        if table_schema.get("file_format") == "parquet":
            logger.warning(
                f"parquet file format was requested for table {table_schema['name']} but ADBC"
                " driver "
                "for postgres was not installed. Read more:"
                " https://dlthub.com/docs/dlt-ecosystem/destinations/postgres#fast-loading-with-arrow-tables-and-parquet"
            )

    return (preferred_loader_file_format, supported_loader_file_formats)


class postgres(Destination[PostgresClientConfiguration, "PostgresClient"]):
    spec = PostgresClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # https://www.postgresql.org/docs/current/limits.html
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "csv", "parquet", "model"]
        caps.loader_file_format_selector = postgres_loader_file_format_selector
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.type_mapper = PostgresTypeMapper
        caps.escape_identifier = escape_postgres_identifier
        # postgres has case sensitive identifiers but by default
        # it folds them to lower case which makes them case insensitive
        # https://stackoverflow.com/questions/20878932/are-postgresql-column-names-case-sensitive
        caps.casefold_identifier = str.lower
        caps.has_case_sensitive_identifiers = True
        caps.escape_literal = escape_postgres_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (2 * EVM_DECIMAL_PRECISION, EVM_DECIMAL_PRECISION)
        caps.max_identifier_length = 63
        caps.max_column_identifier_length = 63
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.supported_merge_strategies = ["delete-insert", "upsert", "scd2"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.sqlglot_dialect = "postgres"

        return caps

    @property
    def client_class(self) -> Type["PostgresClient"]:
        from dlt.destinations.impl.postgres.postgres import PostgresClient

        return PostgresClient

    def __init__(
        self,
        credentials: Union[PostgresCredentials, Dict[str, Any], str] = None,
        create_indexes: bool = True,
        csv_format: Optional[CsvFormatConfiguration] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Postgres destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[PostgresCredentials, Dict[str, Any], str], optional): Credentials to connect to the postgres database. Can be an instance of `PostgresCredentials` or
                a connection string in the format `postgres://user:password@host:port/database`
            create_indexes (bool, optional): Should unique indexes be created
            csv_format (Optional[CsvFormatConfiguration]): Formatting options for csv file format
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            csv_format=csv_format,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


postgres.register()
