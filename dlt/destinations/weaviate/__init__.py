from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination.reference import (
    JobClientBase,
    DestinationClientConfiguration,
)
from dlt.common.destination import DestinationCapabilitiesContext

from dlt.destinations.weaviate.weaviate_adapter import weaviate_adapter
from dlt.destinations.weaviate.configuration import WeaviateClientConfiguration


@with_config(
    spec=WeaviateClientConfiguration,
    sections=(
        known_sections.DESTINATION,
        "weaviate",
    ),
)
def _configure(
    config: WeaviateClientConfiguration = config.value,
) -> WeaviateClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl"]

    caps.max_identifier_length = 200
    caps.max_column_identifier_length = 1024
    caps.max_query_length = 8 * 1024 * 1024
    caps.is_max_query_length_in_bytes = False
    caps.max_text_data_type_length = 8 * 1024 * 1024
    caps.is_max_text_data_type_length_in_bytes = False
    caps.supports_ddl_transactions = False
    caps.naming_convention = "dlt.destinations.weaviate.naming"

    return caps


def client(
    schema: Schema, initial_config: DestinationClientConfiguration = config.value
) -> JobClientBase:
    from dlt.destinations.weaviate.weaviate_client import WeaviateClient

    return WeaviateClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[WeaviateClientConfiguration]:
    return WeaviateClientConfiguration
