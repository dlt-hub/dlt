from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.escape import escape_redshift_identifier, escape_redshift_literal
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, DestinationClientConfiguration

from dlt.destinations.redshift.configuration import RedshiftClientConfiguration


@with_config(spec=RedshiftClientConfiguration, namespaces=("destination", "redshift",))
def _configure(config: RedshiftClientConfiguration = config.value) -> RedshiftClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.update({
        "preferred_loader_file_format": "insert_values",
        "supported_loader_file_formats": ["insert_values"],
        "escape_identifier": escape_redshift_identifier,
        "escape_literal": escape_redshift_literal,
        "max_identifier_length": 127,
        "max_column_identifier_length": 127,
        "max_query_length": 16 * 1024 * 1024,
        "is_max_query_length_in_bytes": True,
        "max_text_data_type_length": 65535,
        "is_max_text_data_type_length_in_bytes": True
    })
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.redshift.redshift import RedshiftClient

    return RedshiftClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return RedshiftClientConfiguration
