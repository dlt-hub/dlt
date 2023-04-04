from typing import Any, Callable, ClassVar, List, Literal

from dlt.common.configuration.utils import serialize_value
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ContainerInjectableContext


# known loader file formats
# jsonl - new line separated json documents
# puae-jsonl - internal extract -> normalize format bases on jsonl
# insert_values - insert SQL statements
# sql - any sql statement
TLoaderFileFormat = Literal["jsonl", "puae-jsonl", "insert_values", "sql"]


@configspec(init=True)
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""
    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: List[TLoaderFileFormat]
    escape_identifier: Callable[[str], str]
    escape_literal: Callable[[Any], Any]
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
    can_create_default: ClassVar[bool] = False

    @staticmethod
    def generic_capabilities(preferred_loader_file_format: TLoaderFileFormat = None) -> "DestinationCapabilitiesContext":
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = preferred_loader_file_format
        caps.supported_loader_file_formats = ["jsonl", "insert_values"]
        caps.escape_identifier = lambda x: x
        caps.escape_literal = lambda x: serialize_value(x)
        caps.max_identifier_length = 65536
        caps.max_column_identifier_length = 65536
        caps.max_query_length = 32 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 1024 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = True
        return caps
