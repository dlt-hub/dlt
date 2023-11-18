from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.qdrant.qdrant_adapter import qdrant_adapter


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

    return caps
