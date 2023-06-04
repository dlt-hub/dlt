from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientDwhConfiguration

from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration


@with_config(spec=FilesystemClientConfiguration, sections=(known_sections.DESTINATION, "filesystem",))
def _configure(config: FilesystemClientConfiguration = config.value) -> FilesystemClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    return DestinationCapabilitiesContext.generic_capabilities("jsonl")


def client(schema: Schema, initial_config: DestinationClientDwhConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.filesystem.filesystem import FilesystemClient

    return FilesystemClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[FilesystemClientConfiguration]:
    return FilesystemClientConfiguration
