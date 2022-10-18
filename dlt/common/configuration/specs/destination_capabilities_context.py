from typing import List, ClassVar, Literal

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext, configspec

# known loader file formats
# jsonl - new line separated json documents
# puae-jsonl - internal extract -> normalize format bases on jsonl
# insert_values - insert SQL statements
TLoaderFileFormat = Literal["jsonl", "puae-jsonl", "insert_values"]


@configspec(init=True)
class DestinationCapabilitiesContext(ContainerInjectableContext):
    """Injectable destination capabilities required for many Pipeline stages ie. normalize"""
    preferred_loader_file_format: TLoaderFileFormat
    supported_loader_file_formats: List[TLoaderFileFormat]
    max_identifier_length: int
    max_column_length: int
    max_query_length: int
    is_max_query_length_in_bytes: bool
    max_text_data_type_length: int
    is_max_text_data_type_length_in_bytes: bool

    # do not allow to create default value, destination caps must be always explicitly inserted into container
    can_create_default: ClassVar[bool] = False
