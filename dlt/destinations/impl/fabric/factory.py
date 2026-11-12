"""
Fabric Warehouse destination for dlt.

- Uses 'fabric' sqlglot dialect
- Uses varchar instead of nvarchar (Fabric doesn't support nvarchar)
- Supports COPY INTO for efficient data loading from staging
"""

from typing import Any, Dict, Type, TYPE_CHECKING, Optional, Union

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_mssql_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema

from dlt.destinations.impl.mssql.factory import MsSqlTypeMapper
from dlt.destinations.impl.fabric.configuration import (
    FabricCredentials,
    FabricClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.fabric.fabric import FabricClient


UTF8_MAX_BYTES_PER_CODE_POINT = 4
FABRIC_VARCHAR_MAX_N = 8000
"""Largest explicit length for `varchar(n)`, anything longer must be `varchar(max)`"""


class FabricTypeMapper(MsSqlTypeMapper):
    """Custom type mapper for Fabric Warehouse - replaces nvarchar with varchar and datetimeoffset with datetime2"""

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        """Override to limit precision to 6 and use datetime2 instead of datetimeoffset"""
        precision = column.get("precision")

        # Fabric only supports precision 0-6 (not 7 like SQL Server)
        if precision is not None and (precision < 0 or precision > 6):
            # Clamp to valid range for Fabric
            precision = 6

        # Fabric doesn't support datetimeoffset, always use datetime2
        # (timezone info is lost, but that's a Fabric limitation)
        if precision is not None:
            return f"datetime2({precision})"
        # Use precision 6 as default (instead of 7 in SQL Server)
        return "datetime2(6)"

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        """Override to use varchar instead of nvarchar and datetime2 instead of datetimeoffset"""
        sc_t = column["data_type"]
        if sc_t in ("json", "text"):
            precision = column.get("precision")
            if precision is None:
                return "varchar(max)"
            # precision counts characters, varchar counts bytes
            length = precision * UTF8_MAX_BYTES_PER_CODE_POINT
            if length > FABRIC_VARCHAR_MAX_N:
                return "varchar(max)"
            return "varchar(%i)" % length

        if sc_t == "time":
            # Fabric requires explicit precision for TIME (0-6)
            precision = min(column.get("precision", 6), 6)
            return "time(%i)" % precision

        # Get base type from parent
        db_type = super().to_destination_type(column, table)

        # Replace any datetimeoffset with datetime2 (Fabric doesn't support datetimeoffset)
        # This should be handled by to_db_datetime_type but just in case
        if "datetimeoffset" in db_type.lower():
            # Extract precision if present, e.g., datetimeoffset(7) -> datetime2(6)
            if "datetimeoffset(" in db_type.lower():
                import re

                match = re.search(r"datetimeoffset\((\d+)\)", db_type, re.IGNORECASE)
                if match:
                    precision = int(match.group(1))
                    precision = min(6, max(0, precision))  # Limit to 0-6
                    db_type = re.sub(
                        r"datetimeoffset\(\d+\)",
                        f"datetime2({precision})",
                        db_type,
                        flags=re.IGNORECASE,
                    )
                else:
                    db_type = db_type.replace("datetimeoffset", "datetime2").replace(
                        "DATETIMEOFFSET", "datetime2"
                    )
            else:
                db_type = db_type.replace("datetimeoffset", "datetime2").replace(
                    "DATETIMEOFFSET", "datetime2"
                )

        return db_type


class fabric(Destination[FabricClientConfiguration, "FabricClient"]):
    """Fabric Warehouse destination with the `fabric` sqlglot dialect and Fabric type mappings"""

    spec = FabricClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        # direct loading with insert_values, parquet only via staging and `COPY INTO`
        caps.preferred_loader_file_format = "insert_values"
        caps.supported_loader_file_formats = ["insert_values", "model"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet"]
        caps.type_mapper = FabricTypeMapper
        caps.insert_values_writer_type = "select_union"

        caps.escape_identifier = escape_postgres_identifier
        caps.escape_literal = escape_mssql_literal
        caps.has_case_sensitive_identifiers = True

        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)

        # NOTE: query, text and insert limits below were set for Synapse dedicated pools
        caps.max_identifier_length = 128
        caps.max_column_identifier_length = 128
        caps.max_query_length = 65536 * 4096
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 2 * 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.max_rows_per_insert = 10000

        caps.supports_transactions = True
        caps.supports_ddl_transactions = False
        caps.supports_create_table_if_not_exists = False
        caps.supports_multiple_statements = True

        caps.timestamp_precision = 6
        # Fabric only supports precision 0-6 for datetime2/time (not 7 like SQL Server)
        caps.max_timestamp_precision = 6
        # no `datetimeoffset`, so `FabricTypeMapper` stores every timestamp as naive `datetime2`
        caps.supports_tz_aware_datetime = False

        caps.supported_merge_strategies = ["delete-insert", "scd2", "upsert", "insert-only", "cdc"]
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]

        caps.sqlglot_dialect = "fabric"

        return caps

    @property
    def client_class(self) -> Type["FabricClient"]:
        from dlt.destinations.impl.fabric.fabric import FabricClient

        return FabricClient

    def __init__(
        self,
        credentials: Union[FabricCredentials, Dict[str, Any]] = None,
        collation: Optional[str] = "Latin1_General_100_BIN2_UTF8",
        create_indexes: bool = False,
        has_case_sensitive_identifiers: bool = True,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Fabric Warehouse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[FabricCredentials, Dict[str, Any]], optional): Credentials to connect to the Fabric Warehouse.
                Can be an instance of `FabricCredentials` or a dictionary with its fields.
            collation (Optional[str]): Database collation for varchar columns.
            create_indexes (bool, optional): Whether `primary_key` and `unique` column hints are applied.
            has_case_sensitive_identifiers (bool, optional): Are identifiers used by the warehouse case sensitive (following the collation)
            destination_name (str, optional): Name of the destination. Defaults to None.
            environment (str, optional): Environment name. Defaults to None.
            **kwargs (Any, optional): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            collation=collation,
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
        config: FabricClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        """Adjust capabilities based on collation case sensitivity"""
        # Modify caps if case sensitive identifiers are requested
        if config.has_case_sensitive_identifiers:
            caps.has_case_sensitive_identifiers = True
            caps.casefold_identifier = str
        return super().adjust_capabilities(caps, config, naming)


# Register the destination
fabric.register()
