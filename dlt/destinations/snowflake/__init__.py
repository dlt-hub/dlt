from typing import Type
from dlt.common.data_writers.escape import escape_bigquery_identifier

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration
from dlt.common.data_writers.escape import escape_snowflake_identifier
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.snowflake.configuration import SnowflakeClientConfiguration


@with_config(spec=SnowflakeClientConfiguration, sections=(known_sections.DESTINATION, "snowflake",))
def _configure(config: SnowflakeClientConfiguration = config.value) -> SnowflakeClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl", "parquet"]
    caps.preferred_staging_file_format = "jsonl"
    caps.supported_staging_file_formats = ["jsonl", "parquet"]
    caps.escape_identifier = escape_snowflake_identifier
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
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.snowflake.snowflake import SnowflakeClient

    return SnowflakeClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return SnowflakeClientConfiguration
