from typing import Type
from dlt.common.data_writers.escape import escape_bigquery_identifier

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, DestinationClientConfiguration

from dlt.destinations.bigquery.configuration import BigQueryClientConfiguration


@with_config(spec=BigQueryClientConfiguration, namespaces=("destination", "bigquery",))
def _configure(config: BigQueryClientConfiguration = config.value) -> BigQueryClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.update({
        "preferred_loader_file_format": "jsonl",
        "supported_loader_file_formats": ["jsonl"],
        "escape_identifier": escape_bigquery_identifier,
        "escape_literal": None,
        "max_identifier_length": 1024,
        "max_column_identifier_length": 300,
        "max_query_length": 1024 * 1024,
        "is_max_query_length_in_bytes": False,
        "max_text_data_type_length": 10 * 1024 * 1024,
        "is_max_text_data_type_length_in_bytes": True
    })
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.bigquery.bigquery import BigQueryClient

    return BigQueryClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return BigQueryClientConfiguration