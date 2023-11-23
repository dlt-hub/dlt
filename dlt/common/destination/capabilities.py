from typing import Any, Callable, ClassVar, List, Literal, Optional, Tuple, Set, get_args

from dlt.common.configuration.utils import serialize_value
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.utils import identity

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.common.wei import EVM_DECIMAL_PRECISION

# known loader file formats
# jsonl - new line separated json documents
# puae-jsonl - internal extract -> normalize format bases on jsonl
# insert_values - insert SQL statements
# sql - any sql statement
TLoaderFileFormat = Literal[
    "jsonl", "puae-jsonl", "insert_values", "sql", "parquet", "reference", "arrow"
]
ALL_SUPPORTED_FILE_FORMATS: Set[TLoaderFileFormat] = set(get_args(TLoaderFileFormat))
# file formats used internally by dlt
INTERNAL_LOADER_FILE_FORMATS: Set[TLoaderFileFormat] = {"puae-jsonl", "sql", "reference", "arrow"}
# file formats that may be chosen by the user
EXTERNAL_LOADER_FILE_FORMATS: Set[TLoaderFileFormat] = (
    set(get_args(TLoaderFileFormat)) - INTERNAL_LOADER_FILE_FORMATS
)


@configspec
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""

    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: List[TLoaderFileFormat]
    preferred_staging_file_format: Optional[TLoaderFileFormat]
    supported_staging_file_formats: List[TLoaderFileFormat]
    escape_identifier: Callable[[str], str]
    escape_literal: Callable[[Any], Any]
    decimal_precision: Tuple[int, int]
    wei_precision: Tuple[int, int]
    max_identifier_length: int
    max_column_identifier_length: int
    max_query_length: int
    is_max_query_length_in_bytes: bool
    max_text_data_type_length: int
    is_max_text_data_type_length_in_bytes: bool
    supports_transactions: bool
    supports_ddl_transactions: bool
    naming_convention: str = "snake_case"
    alter_add_multi_column: bool = True
    supports_truncate_command: bool = True
    schema_supports_numeric_precision: bool = True
    timestamp_precision: int = 6
    max_rows_per_insert: Optional[int] = None

    # do not allow to create default value, destination caps must be always explicitly inserted into container
    can_create_default: ClassVar[bool] = False

    @staticmethod
    def generic_capabilities(
        preferred_loader_file_format: TLoaderFileFormat = None,
    ) -> "DestinationCapabilitiesContext":
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = preferred_loader_file_format
        caps.supported_loader_file_formats = ["jsonl", "insert_values", "parquet"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.escape_identifier = identity
        caps.escape_literal = serialize_value
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
        return caps
