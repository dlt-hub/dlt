from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration

from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration


@with_config(spec=FilesystemClientConfiguration, sections=(known_sections.DESTINATION, "filesystem",))
def _configure(config: FilesystemClientConfiguration = config.value) -> FilesystemClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "jsonl"
    caps.supported_loader_file_formats = ["jsonl"]

    caps.max_identifier_length = 127
    caps.max_column_identifier_length = 127
    caps.max_query_length = 8 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 65536
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = False

    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.filesystem.filesystem import FilesystemClient

    return FilesystemClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return FilesystemClientConfiguration
