from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_dremio_identifier
from dlt.common.destination import DestinationCapabilitiesContext


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = None
    caps.supported_loader_file_formats = []
    caps.preferred_staging_file_format = "parquet"
    caps.supported_staging_file_formats = ["jsonl", "parquet"]
    caps.escape_identifier = escape_dremio_identifier
    # all identifiers are case insensitive but are stored as is
    # https://docs.dremio.com/current/sonar/data-sources
    caps.has_case_sensitive_identifiers = False
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    caps.max_identifier_length = 255
    caps.max_column_identifier_length = 255
    caps.max_query_length = 2 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 16 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_transactions = False
    caps.supports_ddl_transactions = False
    caps.alter_add_multi_column = True
    caps.supports_clone_table = False
    caps.supports_multiple_statements = False
    caps.timestamp_precision = 3
    return caps
