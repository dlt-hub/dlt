from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.typing import ConfigValue
from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext, JobClientBase, DestinationClientConfiguration

from dlt.load.bigquery.configuration import BigQueryClientConfiguration


@with_config(spec=BigQueryClientConfiguration, namespaces=("destination", "bigquery",))
def _configure(config: BigQueryClientConfiguration = ConfigValue) -> BigQueryClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.update({
        "preferred_loader_file_format": "jsonl",
        "supported_loader_file_formats": ["jsonl"],
        "max_identifier_length": 1024,
        "max_column_length": 300,
        "max_query_length": 1024 * 1024,
        "is_max_query_length_in_bytes": False,
        "max_text_data_type_length": 10 * 1024 * 1024,
        "is_max_text_data_type_length_in_bytes": True
    })
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = ConfigValue) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.load.bigquery.bigquery import BigQueryClient

    return BigQueryClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return BigQueryClientConfiguration