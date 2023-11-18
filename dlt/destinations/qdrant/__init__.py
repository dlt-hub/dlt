from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination.reference import (
    JobClientBase,
    DestinationClientConfiguration,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.qdrant.qdrant_adapter import qdrant_adapter

from dlt.destinations.qdrant.configuration import QdrantClientConfiguration


@with_config(
    spec=QdrantClientConfiguration,
    sections=(
        known_sections.DESTINATION,
        "qdrant",
    ),
)
def _configure(
    config: QdrantClientConfiguration = config.value,
) -> QdrantClientConfiguration:
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

    return caps


def client(
    schema: Schema, initial_config: DestinationClientConfiguration = config.value
) -> JobClientBase:
    from dlt.destinations.qdrant.qdrant_client import QdrantClient
    return QdrantClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[QdrantClientConfiguration]:
    return QdrantClientConfiguration
