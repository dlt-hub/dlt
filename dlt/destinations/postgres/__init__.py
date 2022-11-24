from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_postgres_literal
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, DestinationClientConfiguration

from dlt.destinations.postgres.configuration import PostgresClientConfiguration


@with_config(spec=PostgresClientConfiguration, namespaces=("destination", "postgres",))
def _configure(config: PostgresClientConfiguration = config.value) -> PostgresClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    # https://www.postgresql.org/docs/current/limits.html
    caps = DestinationCapabilitiesContext()
    caps.update({
        "preferred_loader_file_format": "insert_values",
        "supported_loader_file_formats": ["insert_values"],
        "escape_identifier": escape_postgres_identifier,
        "escape_literal": escape_postgres_literal,
        "max_identifier_length": 63,
        "max_column_identifier_length": 63,
        "max_query_length": 32 * 1024 * 1024,
        "is_max_query_length_in_bytes": True,
        "max_text_data_type_length": 1024 * 1024 * 1024,
        "is_max_text_data_type_length_in_bytes": True
    })
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.postgres.postgres import PostgresClient

    return PostgresClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return PostgresClientConfiguration
