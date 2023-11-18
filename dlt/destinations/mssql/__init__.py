from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_mssql_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION

from dlt.destinations.mssql.configuration import MsSqlClientConfiguration


@with_config(spec=MsSqlClientConfiguration, sections=(known_sections.DESTINATION, "mssql",))
def _configure(config: MsSqlClientConfiguration = config.value) -> MsSqlClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "insert_values"
    caps.supported_loader_file_formats = ["insert_values"]
    caps.preferred_staging_file_format = None
    caps.supported_staging_file_formats = []
    caps.escape_identifier = escape_postgres_identifier
    caps.escape_literal = escape_mssql_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    # https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-ver16&redirectedfrom=MSDN
    caps.max_identifier_length = 128
    caps.max_column_identifier_length = 128
    caps.max_query_length = 4 * 1024 * 64 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 2 ** 30 - 1
    caps.is_max_text_data_type_length_in_bytes = False
    caps.supports_ddl_transactions = True
    caps.max_rows_per_insert = 1000
    caps.timestamp_precision = 7

    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.mssql.mssql import MsSqlClient

    return MsSqlClient(schema, _configure(initial_config))  # type: ignore[arg-type]


def spec() -> Type[DestinationClientConfiguration]:
    return MsSqlClientConfiguration
