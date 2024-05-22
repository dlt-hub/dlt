from dlt.common.data_writers.escape import (
    escape_bigquery_identifier,
    format_bigquery_datetime_literal,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl", "parquet"]
    caps.preferred_staging_file_format = "parquet"
    caps.supported_staging_file_formats = ["parquet", "jsonl"]
    # BQ limit is 4GB but leave a large headroom since buffered writer does not preemptively check size
    caps.recommended_file_size = int(1024 * 1024 * 1024)
    caps.escape_identifier = escape_bigquery_identifier
    caps.escape_literal = None
    caps.format_datetime_literal = format_bigquery_datetime_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (76, 38)
    caps.max_identifier_length = 1024
    caps.max_column_identifier_length = 300
    caps.max_query_length = 1024 * 1024
    caps.is_max_query_length_in_bytes = False
    caps.max_text_data_type_length = 10 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = False
    caps.supports_clone_table = True

    return caps
