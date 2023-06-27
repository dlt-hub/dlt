from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration

from dlt.destinations.dummy.configuration import DummyClientConfiguration


@with_config(spec=DummyClientConfiguration, sections=(known_sections.DESTINATION, "dummy",))
def _configure(config: DummyClientConfiguration = config.value) -> DummyClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    config = _configure()
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = config.loader_file_format
    caps.supported_loader_file_formats = [config.loader_file_format]
    caps.preferred_staging_file_format = None
    caps.supported_staging_file_formats = []
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
    from dlt.destinations.dummy.dummy import DummyClient

    return DummyClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return DummyClientConfiguration
