from dlt.common.data_writers.escape import escape_postgres_identifier, escape_mssql_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION

from dlt.destinations.impl.synapse.synapse_adapter import synapse_adapter


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()

    caps.preferred_loader_file_format = "insert_values"
    caps.supported_loader_file_formats = ["insert_values"]
    caps.preferred_staging_file_format = "parquet"
    caps.supported_staging_file_formats = ["parquet"]

    caps.insert_values_writer_type = "select_union"  # https://stackoverflow.com/a/77014299

    caps.escape_identifier = escape_postgres_identifier
    caps.escape_literal = escape_mssql_literal

    # Synapse has a max precision of 38
    # https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#DataTypes
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)

    # https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#LimitationsRestrictions
    caps.max_identifier_length = 128
    caps.max_column_identifier_length = 128

    # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-service-capacity-limits#queries
    caps.max_query_length = 65536 * 4096
    caps.is_max_query_length_in_bytes = True

    # nvarchar(max) can store 2 GB
    # https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql?view=sql-server-ver16#nvarchar---n--max--
    caps.max_text_data_type_length = 2 * 1024 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True

    # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-develop-transactions
    caps.supports_transactions = True
    caps.supports_ddl_transactions = False

    # Synapse throws "Some part of your SQL statement is nested too deeply. Rewrite the query or break it up into smaller queries."
    # if number of records exceeds a certain number. Which exact number that is seems not deterministic:
    # in tests, I've seen a query with 12230 records run succesfully on one run, but fail on a subsequent run, while the query remained exactly the same.
    # 10.000 records is a "safe" amount that always seems to work.
    caps.max_rows_per_insert = 10000

    # datetimeoffset can store 7 digits for fractional seconds
    # https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-ver16
    caps.timestamp_precision = 7

    return caps
