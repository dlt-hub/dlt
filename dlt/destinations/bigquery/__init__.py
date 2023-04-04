from typing import Type
from dlt.common.data_writers.escape import escape_bigquery_identifier

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration

from dlt.destinations.bigquery.configuration import BigQueryClientConfiguration


@with_config(spec=BigQueryClientConfiguration, sections=(known_sections.DESTINATION, "bigquery",))
def _configure(config: BigQueryClientConfiguration = config.value) -> BigQueryClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl", "sql"]
    caps.escape_identifier = escape_bigquery_identifier
    caps.escape_literal = None
    caps.max_identifier_length = 1024
    caps.max_column_identifier_length = 300
    caps.max_query_length = 1024 * 1024
    caps.is_max_query_length_in_bytes = False
    caps.max_text_data_type_length = 10 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = False

    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.bigquery.bigquery import BigQueryClient

    return BigQueryClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return BigQueryClientConfiguration