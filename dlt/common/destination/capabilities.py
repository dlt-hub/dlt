from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    ClassVar,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Set,
    Protocol,
    Type,
)
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common.data_types import TDataType
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.exceptions import TerminalValueError
from dlt.common.normalizers.typing import TNamingConventionReferenceArg
from dlt.common.typing import TLoaderFileFormat, get_args
from dlt.common.configuration.utils import serialize_value
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.destination.exceptions import (
    DestinationIncompatibleLoaderFileFormatException,
    DestinationLoadingViaStagingNotSupported,
    DestinationLoadingWithoutStagingNotSupported,
)
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.schema.typing import (
    TColumnSchema,
    TColumnType,
    TTableSchema,
    TLoaderMergeStrategy,
    TTableFormat,
    TLoaderReplaceStrategy,
)
from dlt.common.wei import EVM_DECIMAL_PRECISION

TLoaderParallelismStrategy = Literal["parallel", "table-sequential", "sequential"]
LOADER_FILE_FORMATS: Set[TLoaderFileFormat] = set(get_args(TLoaderFileFormat))


class LoaderFileFormatSelector(Protocol):
    """Selects preferred and supported file formats for a given table schema"""

    @staticmethod
    def __call__(
        preferred_loader_file_format: TLoaderFileFormat,
        supported_loader_file_formats: Sequence[TLoaderFileFormat],
        /,
        *,
        table_schema: TTableSchema,
    ) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]: ...


class MergeStrategySelector(Protocol):
    """Selects right set of merge strategies for a given table schema"""

    @staticmethod
    def __call__(
        supported_merge_strategies: Sequence[TLoaderMergeStrategy],
        /,
        *,
        table_schema: TTableSchema,
    ) -> Sequence["TLoaderMergeStrategy"]: ...


class ReplaceStrategySelector(Protocol):
    """Selects right set of replace strategies for a given table schema"""

    @staticmethod
    def __call__(
        supported_replace_strategies: Sequence[TLoaderReplaceStrategy],
        /,
        *,
        table_schema: TTableSchema,
    ) -> Sequence["TLoaderReplaceStrategy"]: ...


class DataTypeMapper(ABC):
    def __init__(self, capabilities: "DestinationCapabilitiesContext") -> None:
        """Maps dlt data types into destination data types"""
        self.capabilities = capabilities

    @abstractmethod
    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        """Gets destination data type for a particular `column` in prepared `table`"""
        pass

    @abstractmethod
    def from_destination_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        """Gets column type from db type"""
        pass

    @abstractmethod
    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        """Makes sure that dlt type in `column` in prepared `table`  is supported by the destination for a given file format"""
        pass


class UnsupportedTypeMapper(DataTypeMapper):
    """Type Mapper that can't map any type"""

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        raise NotImplementedError("No types are supported, use real type mapper")

    def from_destination_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        raise NotImplementedError("No types are supported, use real type mapper")

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        raise TerminalValueError(
            "No types are supported, use real type mapper", column["data_type"]
        )


@configspec
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""

    # do not allow to create default value, destination caps must be always explicitly inserted into container
    can_create_default: ClassVar[bool] = False

    preferred_loader_file_format: TLoaderFileFormat = None
    supported_loader_file_formats: Sequence[TLoaderFileFormat] = None
    loader_file_format_selector: LoaderFileFormatSelector = None
    """Callable that adapts `preferred_loader_file_format` and `supported_loader_file_formats` at runtime."""
    preferred_table_format: TTableFormat = None
    supported_table_formats: Sequence[TTableFormat] = None
    type_mapper: Optional[Type[DataTypeMapper]] = None
    recommended_file_size: Optional[int] = None
    """Recommended file size in bytes when writing extract/load files"""
    preferred_staging_file_format: Optional[TLoaderFileFormat] = None
    supported_staging_file_formats: Sequence[TLoaderFileFormat] = None
    format_datetime_literal: Callable[..., str] = None
    escape_identifier: Callable[[str], str] = None
    "Escapes table name, column name and other identifiers"
    escape_literal: Callable[[Any], Any] = None
    "Escapes string literal"
    casefold_identifier: Callable[[str], str] = str
    """Casing function applied by destination to represent case insensitive identifiers."""
    has_case_sensitive_identifiers: bool = None
    """Tells if destination supports case sensitive identifiers"""
    decimal_precision: Tuple[int, int] = None
    wei_precision: Tuple[int, int] = None
    max_identifier_length: int = None
    max_column_identifier_length: int = None
    max_query_length: int = None
    is_max_query_length_in_bytes: bool = None
    max_text_data_type_length: int = None
    is_max_text_data_type_length_in_bytes: bool = None
    supports_transactions: bool = None
    supports_ddl_transactions: bool = None
    # use naming convention in the schema
    naming_convention: TNamingConventionReferenceArg = None
    alter_add_multi_column: bool = True
    supports_create_table_if_not_exists: bool = True
    supports_truncate_command: bool = True
    schema_supports_numeric_precision: bool = True
    timestamp_precision: int = 6
    max_rows_per_insert: Optional[int] = None
    insert_values_writer_type: str = "default"
    supports_multiple_statements: bool = True
    supports_clone_table: bool = False
    """Destination supports CREATE TABLE ... CLONE ... statements"""

    max_table_nesting: Optional[int] = None
    """Allows a destination to overwrite max_table_nesting from source"""

    supported_merge_strategies: Sequence[TLoaderMergeStrategy] = None
    merge_strategies_selector: MergeStrategySelector = None
    supported_replace_strategies: Sequence[TLoaderReplaceStrategy] = None
    replace_strategies_selector: ReplaceStrategySelector = None

    max_parallel_load_jobs: Optional[int] = None
    """The destination can set the maximum amount of parallel load jobs being executed"""
    loader_parallelism_strategy: Optional[TLoaderParallelismStrategy] = None
    """The destination can override the parallelism strategy"""

    max_query_parameters: Optional[int] = None
    """The maximum number of parameters that can be supplied in a single parametrized query"""

    supports_native_boolean: bool = True
    """The destination supports a native boolean type, otherwise bool columns are usually stored as integers"""

    supports_nested_types: bool = False
    """Tells if destination can write nested types, currently only destinations storing parquet are supported"""

    enforces_nulls_on_alter: bool = True
    """Tells if destination enforces null constraints when adding NOT NULL columns to existing tables"""

    sqlglot_dialect: Optional[TSqlGlotDialect] = None
    """The SQL dialect used by sqlglot to transpile a query to match the destination syntax."""

    parquet_format: Optional[ParquetFormatConfiguration] = None
    """Parquet format preferred by this destination"""

    def generates_case_sensitive_identifiers(self) -> bool:
        """Tells if capabilities as currently adjusted, will generate case sensitive identifiers"""
        # must have case sensitive support and folding function must preserve casing
        return self.has_case_sensitive_identifiers and self.casefold_identifier is str

    @staticmethod
    def generic_capabilities(
        preferred_loader_file_format: TLoaderFileFormat = None,
        naming_convention: TNamingConventionReferenceArg = None,
        loader_file_format_selector: LoaderFileFormatSelector = None,
        supported_table_formats: Sequence[TTableFormat] = None,
        supported_merge_strategies: Sequence[TLoaderMergeStrategy] = None,
        merge_strategies_selector: MergeStrategySelector = None,
    ) -> "DestinationCapabilitiesContext":
        from dlt.common.data_writers.escape import format_datetime_literal

        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = preferred_loader_file_format
        caps.supported_loader_file_formats = ["jsonl", "insert_values", "parquet", "csv", "model"]
        caps.loader_file_format_selector = loader_file_format_selector
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.naming_convention = naming_convention or caps.naming_convention
        caps.escape_identifier = str
        caps.supported_table_formats = supported_table_formats or []
        caps.escape_literal = serialize_value
        caps.casefold_identifier = str
        caps.has_case_sensitive_identifiers = True
        caps.format_datetime_literal = format_datetime_literal
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.wei_precision = (EVM_DECIMAL_PRECISION, 0)
        caps.max_identifier_length = 65536
        caps.max_column_identifier_length = 65536
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        caps.supports_transactions = True
        caps.supports_multiple_statements = True
        caps.supported_merge_strategies = supported_merge_strategies or []
        caps.merge_strategies_selector = merge_strategies_selector
        return caps

    def get_type_mapper(self, *args: Any, **kwargs: Any) -> DataTypeMapper:
        return self.type_mapper(self, *args, **kwargs)


def merge_caps_file_formats(
    destination: str,
    staging: str,
    dest_caps: DestinationCapabilitiesContext,
    stage_caps: DestinationCapabilitiesContext,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    """Merges preferred and supported file formats from destination and staging.
    Returns new preferred file format and all possible formats.
    """
    possible_file_formats = dest_caps.supported_loader_file_formats
    if stage_caps:
        if not dest_caps.supported_staging_file_formats:
            raise DestinationLoadingViaStagingNotSupported(destination)
        possible_file_formats = [
            f
            for f in dest_caps.supported_staging_file_formats
            if f in stage_caps.supported_loader_file_formats
        ]
    if len(possible_file_formats) == 0:
        raise DestinationIncompatibleLoaderFileFormatException(
            destination, staging, None, possible_file_formats
        )
    if not stage_caps:
        if not dest_caps.preferred_loader_file_format:
            raise DestinationLoadingWithoutStagingNotSupported(destination)
        requested_file_format = dest_caps.preferred_loader_file_format
    elif stage_caps and dest_caps.preferred_staging_file_format in possible_file_formats:
        requested_file_format = dest_caps.preferred_staging_file_format
    else:
        requested_file_format = possible_file_formats[0] if len(possible_file_formats) > 0 else None
    return requested_file_format, possible_file_formats
