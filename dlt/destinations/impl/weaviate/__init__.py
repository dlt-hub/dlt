from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.weaviate.weaviate_adapter import weaviate_adapter


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl"]

    caps.max_identifier_length = 200
    caps.max_column_identifier_length = 1024
    caps.max_query_length = 8 * 1024 * 1024
    caps.is_max_query_length_in_bytes = False
    caps.max_text_data_type_length = 8 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = False
    caps.supports_ddl_transactions = False
    caps.naming_convention = "dlt.destinations.impl.weaviate.naming"

    return caps
