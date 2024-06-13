from dlt.common.data_writers.escape import escape_hive_identifier, format_bigquery_datetime_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl", "parquet"]
    caps.preferred_staging_file_format = "parquet"
    caps.supported_staging_file_formats = ["parquet", "jsonl"]
    # BigQuery is by default case sensitive but that cannot be turned off for a dataset
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity
    caps.escape_identifier = escape_hive_identifier
    caps.escape_literal = None
    caps.has_case_sensitive_identifiers = True
    caps.casefold_identifier = str
    # BQ limit is 4GB but leave a large headroom since buffered writer does not preemptively check size
    caps.recommended_file_size = int(1024 * 1024 * 1024)
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
    caps.schema_supports_numeric_precision = False  # no precision information in BigQuery

    return caps
