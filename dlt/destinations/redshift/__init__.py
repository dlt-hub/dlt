from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.escape import escape_redshift_identifier, escape_redshift_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE

from dlt.destinations.redshift.configuration import RedshiftClientConfiguration


@with_config(spec=RedshiftClientConfiguration, sections=(known_sections.DESTINATION, "redshift",))
def _configure(config: RedshiftClientConfiguration = config.value) -> RedshiftClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "insert_values"
    caps.supported_loader_file_formats = ["insert_values"]
    caps.preferred_staging_file_format = "jsonl"
    caps.supported_staging_file_formats = ["jsonl", "parquet"]
    caps.escape_identifier = escape_redshift_identifier
    caps.escape_literal = escape_redshift_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    caps.max_identifier_length = 127
    caps.max_column_identifier_length = 127
    caps.max_query_length = 16 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 65535
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = True
    caps.alter_add_multi_column = False

    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.redshift.redshift import RedshiftClient

    return RedshiftClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return RedshiftClientConfiguration
