from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_duckdb_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.duckdb.configuration import DuckDbClientConfiguration


@with_config(spec=DuckDbClientConfiguration, sections=(known_sections.DESTINATION, "duckdb",))
def _configure(config: DuckDbClientConfiguration = config.value) -> DuckDbClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "insert_values"
    caps.supported_loader_file_formats = ["insert_values", "parquet", "jsonl"]
    caps.preferred_staging_file_format = None
    caps.supported_staging_file_formats = []
    caps.escape_identifier = escape_postgres_identifier
    caps.escape_literal = escape_duckdb_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    caps.max_identifier_length = 65536
    caps.max_column_identifier_length = 65536
    caps.max_query_length = 32 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 1024 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = True
    caps.alter_add_multi_column = False
    caps.supports_truncate_command = False

    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.duckdb.duck import DuckDbClient

    return DuckDbClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return DuckDbClientConfiguration
