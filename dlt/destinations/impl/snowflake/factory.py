from typing import Any, Dict, Sequence, Tuple, Type, Union, TYPE_CHECKING, Optional, cast

from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_snowflake_identifier
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema.typing import TColumnSchema, TColumnType

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeCredentials,
    SnowflakeClientConfiguration,
)

if TYPE_CHECKING:
    from dlt.common.libs.ibis import BaseBackend
    from dlt.common.schema import Schema
    from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient


class SnowflakeTypeMapper(TypeMapperImpl):
    BIGINT_PRECISION = 19
    sct_to_unbound_dbt = {
        "json": "VARIANT",
        "text": "VARCHAR",
        "double": "FLOAT",
        "bool": "BOOLEAN",
        "date": "DATE",
        "bigint": f"NUMBER({BIGINT_PRECISION},0)",  # Snowflake has no integer types
        "binary": "BINARY",
        "time": "TIME",
        "decimal": "DECIMAL",
    }

    sct_to_dbt = {
        "text": "VARCHAR(%i)",
        "decimal": "NUMBER(%i,%i)",
        "time": "TIME(%i)",
        "wei": "NUMBER(%i,%i)",
    }

    dbt_to_sct = {
        "VARCHAR": "text",
        "FLOAT": "double",
        "BOOLEAN": "bool",
        "DATE": "date",
        "TIMESTAMP_LTZ": "timestamp",
        "TIMESTAMP_TZ": "timestamp",
        "BINARY": "binary",
        "VARIANT": "json",
        # structured types reflect via information_schema as bare ARRAY/OBJECT/MAP
        "ARRAY": "json",
        "OBJECT": "json",
        "MAP": "json",
        "TIME": "time",
        "DECFLOAT": "decimal",
        "DECIMAL": "decimal",
    }

    def __init__(
        self,
        capabilities: DestinationCapabilitiesContext,
        use_decfloat: bool = False,
        use_timestamp_tz: bool = False,
    ) -> None:
        super().__init__(capabilities)
        self.use_decfloat = use_decfloat
        self.use_timestamp_tz = use_timestamp_tz

    def from_destination_type(
        self, db_type: str, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> TColumnType:
        if db_type == "NUMBER":
            if precision == self.BIGINT_PRECISION and scale == 0:
                return dict(data_type="bigint")
            elif (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
            return dict(data_type="decimal", precision=precision, scale=scale)
        if db_type == "TIMESTAMP_NTZ":
            return dict(data_type="timestamp", precision=precision, scale=scale, timezone=False)
        return super().from_destination_type(db_type, precision, scale)

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        timezone = column.get("timezone", True)
        precision = column.get("precision")

        if not timezone:
            timestamp = "TIMESTAMP_NTZ"
        elif self.use_timestamp_tz:
            timestamp = "TIMESTAMP_TZ"
        else:
            timestamp = "TIMESTAMP_LTZ"

        # append precision if specified and valid
        if precision is not None:
            if 0 <= precision <= self.capabilities.max_timestamp_precision:
                timestamp += f"({precision})"
            else:
                column_name = column["name"]
                table_name = table["name"]
                raise TerminalValueError(
                    f"Snowflake does not support `{precision=:}` for timestamp column"
                    f" `{column_name}` in table `{table_name}`"
                )

        return timestamp

    def decimal_precision(
        self, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> Optional[Tuple[int, int]]:
        # when use_decfloat is enabled, unbound decimals map to DECFLOAT (no precision)
        if self.use_decfloat and precision is None and scale is None:
            return None
        return super().decimal_precision(precision, scale)

    def to_db_decimal_type(self, column: TColumnSchema) -> str:
        precision_tup = self.decimal_precision(column.get("precision"), column.get("scale"))
        if self.use_decfloat and precision_tup is None:
            return "DECFLOAT"
        return super().to_db_decimal_type(column)

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        nested_type = column.get("x-nested-type")
        if (
            self.capabilities.supports_nested_types
            and column["data_type"] == "json"
            and nested_type
        ):
            from dlt.common.libs.pyarrow import deserialize_type

            return self._to_nested_db_type(deserialize_type(cast(str, nested_type)), table)
        return super().to_destination_type(column, table)

    def _to_nested_db_type(self, dtype: Any, table: PreparedTableSchema) -> str:
        """Maps an arrow nested `DataType` to a Snowflake structured type, recursing into elements."""
        from dlt.common.libs.pyarrow import pyarrow, get_column_type_from_py_arrow

        if (
            pyarrow.types.is_list(dtype)
            or pyarrow.types.is_large_list(dtype)
            or pyarrow.types.is_fixed_size_list(dtype)
        ):
            return f"ARRAY({self._to_nested_db_type(dtype.value_type, table)})"
        if pyarrow.types.is_struct(dtype):
            # quote field names: structured field matching is case-sensitive and names may need escaping
            fields = ", ".join(
                f"{escape_snowflake_identifier(dtype.field(i).name)}"
                f" {self._to_nested_db_type(dtype.field(i).type, table)}"
                for i in range(dtype.num_fields)
            )
            return f"OBJECT({fields})"
        if pyarrow.types.is_map(dtype):
            return (
                f"MAP({self._to_nested_db_type(dtype.key_type, table)},"
                f" {self._to_nested_db_type(dtype.item_type, table)})"
            )
        leaf: TColumnSchema = {"name": "", **get_column_type_from_py_arrow(dtype)}
        return self.to_destination_type(leaf, table)


class snowflake(Destination[SnowflakeClientConfiguration, "SnowflakeClient"]):
    spec = SnowflakeClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.supports_session_timezone = True
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl", "parquet", "csv", "model"]
        caps.preferred_staging_file_format = "jsonl"
        caps.supported_staging_file_formats = ["jsonl", "parquet", "csv"]
        caps.type_mapper = SnowflakeTypeMapper
        # snowflake is case sensitive but all unquoted identifiers are upper cased
        # so upper case identifiers are considered case insensitive
        caps.escape_identifier = escape_snowflake_identifier
        # dlt is configured to create case insensitive identifiers
        # note that case sensitive naming conventions will change this setting to "str" (case sensitive)
        caps.casefold_identifier = str.upper
        caps.has_case_sensitive_identifiers = True
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 2 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 16 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.alter_add_multi_column = True
        caps.supports_clone_table = True
        caps.supported_merge_strategies = [
            "delete-insert",
            "upsert",
            "scd2",
            "insert-only",
            "cdc",
        ]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
            "insert-from-staging",
            "staging-optimized",
        ]
        caps.timestamp_precision = 6
        caps.max_timestamp_precision = 9
        caps.sqlglot_dialect = "snowflake"

        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: SnowflakeClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        caps.supports_nested_types = config.use_nested_types
        return super().adjust_capabilities(caps, config, naming)

    @property
    def client_class(self) -> Type["SnowflakeClient"]:
        from dlt.destinations.impl.snowflake.snowflake import SnowflakeClient

        return SnowflakeClient

    def create_ibis_backend(
        self, client: "SnowflakeClient", read_only: bool = False, schemas: "Sequence[Schema]" = ()
    ) -> "BaseBackend":
        """Create an ibis snowflake backend for the client's dataset."""
        from dlt.helpers.ibis import ibis

        return ibis.snowflake.connect(
            schema=client.sql_client.fully_qualified_dataset_name(),
            **client.config.credentials.to_connector_params(),
            create_object_udfs=False,
        )

    def __init__(
        self,
        credentials: Union[SnowflakeCredentials, Dict[str, Any], str] = None,
        stage_name: Optional[str] = None,
        keep_staged_files: bool = True,
        csv_format: Optional[CsvFormatConfiguration] = None,
        query_tag: Optional[str] = None,
        create_indexes: bool = False,
        use_decfloat: bool = False,
        use_nested_types: bool = False,
        use_timestamp_tz: bool = False,
        enable_atomic_swap: bool = False,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Snowflake destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[SnowflakeCredentials, Dict[str, Any], str], optional): Credentials to connect to the snowflake database. Can be an instance of `SnowflakeCredentials` or
                a connection string in the format `snowflake://user:password@host:port/database`
            stage_name (Optional[str], optional): Name of an existing stage to use for loading data. Default uses implicit stage per table
            keep_staged_files (bool, optional): Whether to delete or keep staged files after loading
            csv_format (Optional[CsvFormatConfiguration]): Optional csv format configuration
            query_tag (Optional[str]): A template with placeholders used to tag Snowflake sessions for dlt operations
            create_indexes (bool, optional): Whether UNIQUE or PRIMARY KEY constrains should be created
            use_decfloat (bool, optional): Whether to use DECFLOAT type for unbound decimals. DECFLOAT stores
                exact decimal values with up to 36 significant digits and a dynamic exponent.
                Only works with text-based staging formats (jsonl, csv) - not parquet.
            use_nested_types (bool, optional): Whether to create arrow-nested `json` columns as native
                ARRAY/OBJECT (structured) types instead of VARIANT. Requires loading via parquet.
            use_timestamp_tz (bool, optional): Whether to create timezone-aware timestamps as TIMESTAMP_TZ,
                which stores the offset written with each value, instead of TIMESTAMP_LTZ. Columns of
                tables that already exist keep the type they were created with.
            enable_atomic_swap (bool, optional): Whether to use atomic swap when replacing with replace strategy `staging-optimized`.
            destination_name (str, optional): Name of the destination. Defaults to None.
            environment (str, optional): Environment name. Defaults to None.
            **kwargs (Any, optional): Additional arguments forwarded to the destination config
        """
        super().__init__(
            credentials=credentials,
            stage_name=stage_name,
            keep_staged_files=keep_staged_files,
            csv_format=csv_format,
            query_tag=query_tag,
            create_indexes=create_indexes,
            use_decfloat=use_decfloat,
            use_nested_types=use_nested_types,
            use_timestamp_tz=use_timestamp_tz,
            enable_atomic_swap=enable_atomic_swap,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


snowflake.register()
