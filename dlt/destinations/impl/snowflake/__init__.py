from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_snowflake_identifier
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl", "parquet"]
    caps.preferred_staging_file_format = "jsonl"
    caps.supported_staging_file_formats = ["jsonl", "parquet"]
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
    return caps
