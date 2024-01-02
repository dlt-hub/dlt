from dlt.common.data_writers.escape import escape_postgres_identifier, escape_duckdb_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "parquet"
    caps.supported_loader_file_formats = ["parquet", "insert_values", "jsonl"]
    caps.escape_identifier = escape_postgres_identifier
    caps.escape_literal = escape_duckdb_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    caps.max_identifier_length = 65536
    caps.max_column_identifier_length = 65536
    caps.max_query_length = 512 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 1024 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = False
    caps.alter_add_multi_column = False
    caps.supports_truncate_command = False

    return caps
