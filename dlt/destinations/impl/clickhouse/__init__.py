from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.data_writers.escape import escape_clickhouse_identifier, escape_clickhouse_literal
from dlt.common.destination import DestinationCapabilitiesContext


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()

    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl", "parquet", "insert_values"]
    caps.preferred_staging_file_format = "jsonl"
    caps.supported_staging_file_formats = ["parquet", "jsonl"]

    caps.escape_identifier = escape_clickhouse_identifier
    caps.escape_literal = escape_clickhouse_literal

    # Muse use 'Decimal128' with these defaults.
    # https://clickhouse.com/docs/en/sql-reference/data-types/decimal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)

    # https://clickhouse.com/docs/en/operations/settings/settings#max_query_size
    caps.is_max_query_length_in_bytes = True
    caps.max_query_length = 262144

    # Clickhouse has limited support for transactional semantics, especially for `ReplicatedMergeTree`,
    # the default ClickHouse cloud engine.
    # https://clickhouse-driver.readthedocs.io/en/latest/dbapi.html#clickhouse_driver.dbapi.connection.Connection.commit
    # https://clickhouse.com/docs/en/guides/developer/transactional#transactions-commit-and-rollback
    caps.supports_ddl_transactions = False
    caps.supports_transactions = False

    return caps
