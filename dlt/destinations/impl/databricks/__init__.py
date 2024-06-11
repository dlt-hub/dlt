from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_writers.escape import escape_databricks_identifier, escape_databricks_literal
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = None
    caps.supported_loader_file_formats = []
    caps.preferred_staging_file_format = "parquet"
    caps.supported_staging_file_formats = ["jsonl", "parquet"]
    caps.escape_identifier = escape_databricks_identifier
    # databricks identifiers are case insensitive and stored in lower case
    # https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
    caps.escape_literal = escape_databricks_literal
    caps.casefold_identifier = str.lower
    caps.has_case_sensitive_identifiers = False
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    caps.max_identifier_length = 255
    caps.max_column_identifier_length = 255
    caps.max_query_length = 2 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 16 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = False
    caps.supports_truncate_command = True
    # caps.supports_transactions = False
    caps.alter_add_multi_column = True
    caps.supports_multiple_statements = False
    caps.supports_clone_table = True
    return caps
