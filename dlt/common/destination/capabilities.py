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
    get_args,
)
from dlt.common.normalizers.typing import TNamingConventionReferenceArg
from dlt.common.typing import TLoaderFileFormat
from dlt.common.configuration.utils import serialize_value
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.destination.exceptions import (
    DestinationIncompatibleLoaderFileFormatException,
    DestinationLoadingViaStagingNotSupported,
    DestinationLoadingWithoutStagingNotSupported,
)
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION

TLoaderParallelismStrategy = Literal["parallel", "table-sequential", "sequential"]
ALL_SUPPORTED_FILE_FORMATS: Set[TLoaderFileFormat] = set(get_args(TLoaderFileFormat))


class LoaderFileFormatAdapter(Protocol):
    """Callback protocol for `loader_file_format_adapter` capability."""

    def __call__(
        self,
        preferred_loader_file_format: TLoaderFileFormat,
        supported_loader_file_formats: Sequence[TLoaderFileFormat],
        /,
        *,
        table_schema: "TTableSchema",  # type: ignore[name-defined] # noqa: F821
    ) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]: ...


@configspec
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""

    preferred_loader_file_format: TLoaderFileFormat = None
    supported_loader_file_formats: Sequence[TLoaderFileFormat] = None
    loader_file_format_adapter: LoaderFileFormatAdapter = None
    """Callable that adapts `preferred_loader_file_format` and `supported_loader_file_formats` at runtime."""
    supported_table_formats: Sequence["TTableFormat"] = None  # type: ignore[name-defined] # noqa: F821
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

    supported_merge_strategies: Sequence["TLoaderMergeStrategy"] = None  # type: ignore[name-defined] # noqa: F821
    # TODO: also add `supported_replace_strategies` capability

    # do not allow to create default value, destination caps must be always explicitly inserted into container
    can_create_default: ClassVar[bool] = False

    max_parallel_load_jobs: Optional[int] = None
    """The destination can set the maxium amount of parallel load jobs being executed"""
    loader_parallelism_strategy: Optional[TLoaderParallelismStrategy] = None
    """The destination can override the parallelism strategy"""

    def generates_case_sensitive_identifiers(self) -> bool:
        """Tells if capabilities as currently adjusted, will generate case sensitive identifiers"""
        # must have case sensitive support and folding function must preserve casing
        return self.has_case_sensitive_identifiers and self.casefold_identifier is str

    @staticmethod
    def generic_capabilities(
        preferred_loader_file_format: TLoaderFileFormat = None,
        naming_convention: TNamingConventionReferenceArg = None,
        loader_file_format_adapter: LoaderFileFormatAdapter = None,
        supported_table_formats: Sequence["TTableFormat"] = None,  # type: ignore[name-defined] # noqa: F821
        supported_merge_strategies: Sequence["TLoaderMergeStrategy"] = None,  # type: ignore[name-defined] # noqa: F821
    ) -> "DestinationCapabilitiesContext":
        from dlt.common.data_writers.escape import format_datetime_literal

        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = preferred_loader_file_format
        caps.supported_loader_file_formats = ["jsonl", "insert_values", "parquet", "csv"]
        caps.loader_file_format_adapter = loader_file_format_adapter
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
        return caps


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
