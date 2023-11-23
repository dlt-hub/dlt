from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION


def capabilities() -> DestinationCapabilitiesContext:
    # https://www.postgresql.org/docs/current/limits.html
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "insert_values"
    caps.supported_loader_file_formats = ["insert_values"]
    caps.preferred_staging_file_format = None
    caps.supported_staging_file_formats = []
    caps.escape_identifier = escape_postgres_identifier
    caps.escape_literal = escape_postgres_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (2 * EVM_DECIMAL_PRECISION, EVM_DECIMAL_PRECISION)
    caps.max_identifier_length = 63
    caps.max_column_identifier_length = 63
    caps.max_query_length = 32 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 1024 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = True

    return caps
