import typing as t

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.utils import serialize_value
from dlt.common.utils import identity
from dlt.common.wei import EVM_DECIMAL_PRECISION

# known loader file formats
# jsonl - new line separated json documents
# puae-jsonl - internal extract -> normalize format bases on jsonl
# insert_values - insert SQL statements
# sql - any sql statement
TLoaderFileFormat = t.Literal["jsonl", "puae-jsonl", "insert_values", "sql", "parquet", "reference"]
# file formats used internally by dlt
INTERNAL_LOADER_FILE_FORMATS: t.Set[TLoaderFileFormat] = {"puae-jsonl", "sql", "reference"}
# file formats that may be chosen by the user
EXTERNAL_LOADER_FILE_FORMATS: t.Set[TLoaderFileFormat] = (
    set(t.get_args(TLoaderFileFormat)) - INTERNAL_LOADER_FILE_FORMATS
)


@configspec(init=True)
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""

    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: t.List[TLoaderFileFormat]
    preferred_staging_file_format: t.Optional[TLoaderFileFormat]
    supported_staging_file_formats: t.List[TLoaderFileFormat]
    escape_identifier: t.Callable[[str], str]
    escape_literal: t.Callable[[t.Any], t.Any]
    decimal_precision: t.Tuple[int, int]
    wei_precision: t.Tuple[int, int]
    max_identifier_length: int
    max_column_identifier_length: int
    max_query_length: int
    is_max_query_length_in_bytes: bool
    max_text_data_type_length: int
    is_max_text_data_type_length_in_bytes: bool
    supports_ddl_transactions: bool
    naming_convention: str = "snake_case"
    alter_add_multi_column: bool = True

    # do not allow to create default value, destination caps must be always explicitly inserted into container
    can_create_default: t.ClassVar[bool] = False

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
        return caps
