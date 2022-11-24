from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config
from dlt.common.configuration.accessors import config
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, DestinationClientConfiguration

from dlt.destinations.dummy.configuration import DummyClientConfiguration


@with_config(spec=DummyClientConfiguration, namespaces=("destination", "dummy",))
def _configure(config: DummyClientConfiguration = config.value) -> DummyClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    config = _configure()
    caps = DestinationCapabilitiesContext()
    caps.update({
        "preferred_loader_file_format": config.loader_file_format,
        "supported_loader_file_formats": [config.loader_file_format],
        "max_identifier_length": 127,
        "max_column_identifier_length": 127,
        "max_query_length": 8 * 1024 * 1024,
        "is_max_query_length_in_bytes": True,
        "max_text_data_type_length": 65535,
        "is_max_text_data_type_length_in_bytes": True
    })
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.dummy.dummy import DummyClient

    return DummyClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return DummyClientConfiguration
