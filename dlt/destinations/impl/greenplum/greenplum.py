import typing as t

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.data_writers.escape import (
    escape_postgres_identifier,
    escape_postgres_literal,
)
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.wei import EVM_DECIMAL_PRECISION
from dlt.destinations.impl.postgres.postgres import PostgresClient
from dlt.destinations.impl.postgres.postgres_adapter import GEOMETRY_HINT, SRID_HINT
from dlt.destinations.impl.greenplum.configuration import (
    GreenplumCredentials,
    GreenplumClientConfiguration,
)
from dlt.destinations.type_mapping import TypeMapperImpl


class GreenplumTypeMapper(TypeMapperImpl):
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
        "geometry": "geometry",
    }

    sct_to_dbt = {
        "text": "varchar(%i)",
        "timestamp": "timestamp (%i) with time zone",
        "decimal": "numeric(%i,%i)",
        "time": "time (%i) without time zone",
        "wei": "numeric(%i,%i)",
        "geometry": "geometry(%s,%i)",
    }

    def to_db_integer_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """Maps DLT integer types to Greenplum database types."""
        data_type = column["data_type"]
        if data_type == "bigint":
            if column.get("precision") == 16:
                return "smallint"
            return "bigint"
        elif data_type == "integer":
            return "integer"
        elif data_type == "smallint":
            return "smallint"
        return self.sct_to_unbound_dbt.get(data_type, "bigint")

    def to_db_datetime_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        if column.get("precision"):
            return f"timestamp ({column['precision']}) with time zone"
        return "timestamp with time zone"

    def to_db_time_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """Maps DLT time type with precision to Greenplum time type."""
        precision = column.get("precision")
        if precision is not None:
            return f"time ({precision}) without time zone"
        return "time without time zone"

    def to_db_decimal_type(self, column: TColumnSchema) -> str:
        if column.get("precision"):
            if column.get("scale"):
                return f"numeric({column['precision']},{column['scale']})"
            return f"numeric({column['precision']})"
        return "numeric(38,9)"

    def to_db_varchar_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """Maps DLT string type with precision to Greenplum varchar type."""
        length = column.get("precision")
        if length is not None:
            return f"varchar({length})"
        return "varchar"

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        sc_t = column["data_type"]
        if sc_t == "bigint":
            return self.to_db_integer_type(column, table)
        elif sc_t == "timestamp":
            return self.to_db_datetime_type(column, table)
        elif sc_t == "time":
            return self.to_db_time_type(column, table)
        elif sc_t == "decimal":
            return self.to_db_decimal_type(column)
        elif sc_t == "wei":
            return "numeric(156,78)"
        elif sc_t == "text":
            return self.to_db_varchar_type(column, table)
        elif sc_t == "geometry":
            srid = column.get("srid", 4326)
            geometry_type = column.get("geometry_type", "Geometry")
            return f"geometry({geometry_type},{srid})"
        elif sc_t in self.sct_to_unbound_dbt:
            return self.sct_to_unbound_dbt[sc_t]
        else:
            return super().to_destination_type(column, table)


class GreenplumClient(PostgresClient):
    def __init__(
        self,
        schema: Schema,
        config: GreenplumClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ):
        super().__init__(schema, config, capabilities)
        self.config = config  # Explicit override for typing

    def _get_table_update_sql(
        self,
        table_name: str,
        table_updates: t.Sequence[TColumnSchema],
        table_exists: bool,
    ) -> t.List[str]:
        """Override to add Greenplum storage and distribution parameters when creating a table."""
        # Get base SQL from parent class
        sql_statements = super()._get_table_update_sql(
            table_name, table_updates, table_exists
        )

        # Only add storage parameters when creating a new table
        if not table_exists and sql_statements:
            create_sql = sql_statements[0]

            # Build storage parameters list
            storage_params = []
            if self.config.appendonly:
                storage_params.append("appendonly=true")
            if self.config.blocksize:
                storage_params.append(f"blocksize={self.config.blocksize}")
            if self.config.compresstype:
                storage_params.append(f"compresstype={self.config.compresstype}")
            if self.config.compresslevel:
                storage_params.append(f"compresslevel={self.config.compresslevel}")
            if self.config.orientation:
                storage_params.append(f"orientation={self.config.orientation}")

            # Add storage and distribution parameters before the ending semicolon
            if storage_params:
                create_sql = create_sql.rstrip(";")
                create_sql += f" WITH ({', '.join(storage_params)})"

                # Add distribution clause as a comment to make it compatible with PostgreSQL dialect
                if self.config.distribution_key:
                    dist_key = self.config.distribution_key
                    escaped_dist_key = escape_postgres_identifier(dist_key)
                    create_sql += f" /* DISTRIBUTED BY ({escaped_dist_key}) */"
                else:
                    create_sql += " /* DISTRIBUTED RANDOMLY */"

                create_sql += ";"

                # Update the SQL statement in the result list
                sql_statements[0] = create_sql

        return sql_statements


class greenplum(Destination[GreenplumClientConfiguration, GreenplumClient]):
    spec = GreenplumClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # https://www.postgresql.org/docs/current/limits.html
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "csv"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.type_mapper = GreenplumTypeMapper
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
    def client_class(self) -> t.Type[GreenplumClient]:
        return GreenplumClient

    def __init__(
        self,
        credentials: t.Union[GreenplumCredentials, t.Dict[str, t.Any], str] = None,
        create_indexes: bool = True,
        appendonly: bool = True,
        blocksize: int = 32768,
        compresstype: str = "zstd",
        compresslevel: int = 4,
        orientation: str = "column",
        distribution_key: str = "_dlt_id",
        csv_format: t.Optional[CsvFormatConfiguration] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Greenplum destination for use in a pipeline.

        Args:
            credentials: Credentials for connecting to the Greenplum database
            create_indexes: Whether to create unique indexes
            appendonly: Whether to use appendonly tables
            blocksize: Data block size
            compresstype: Compression type (zstd, zlib, rle_type, quicklz)
            compresslevel: Compression level
            orientation: Table orientation (row, column)
            distribution_key: Data distribution key (default _dlt_id)
            csv_format: CSV format settings
            **kwargs: Additional configuration arguments
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            appendonly=appendonly,
            blocksize=blocksize,
            compresstype=compresstype,
            compresslevel=compresslevel,
            orientation=orientation,
            distribution_key=distribution_key,
            csv_format=csv_format,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


greenplum.register()
