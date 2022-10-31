from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.typing import ConfigValue
from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, DestinationClientConfiguration

from dlt.load.dummy.configuration import DummyClientConfiguration


@with_config(spec=DummyClientConfiguration, namespaces=("destination", "dummy",))
def _configure(config: DummyClientConfiguration = ConfigValue) -> DummyClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    config = _configure()
    caps = DestinationCapabilitiesContext()
    caps.update({
        "preferred_loader_file_format": config.loader_file_format,
        "supported_loader_file_formats": [config.loader_file_format],
        "max_identifier_length": 127,
        "max_column_length": 127,
        "max_query_length": 8 * 1024 * 1024,
        "is_max_query_length_in_bytes": True,
        "max_text_data_type_length": 65535,
        "is_max_text_data_type_length_in_bytes": True
    })
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = ConfigValue) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.load.dummy.dummy import DummyClient

    return DummyClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return DummyClientConfiguration
