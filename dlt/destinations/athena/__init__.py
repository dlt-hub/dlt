from typing import Type

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.schema.schema import Schema
from dlt.common.data_writers.escape import escape_athena_identifier
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION

from dlt.destinations.athena.configuration import AthenaClientConfiguration
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration

@with_config(spec=AthenaClientConfiguration, sections=(known_sections.DESTINATION, "athena",))
def _configure(config: AthenaClientConfiguration = config.value) -> AthenaClientConfiguration:
    return config

def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    # athena only supports loading from staged files on s3 for now
    caps.preferred_loader_file_format = None
    caps.supported_loader_file_formats = []
    caps.preferred_staging_file_format = "parquet"
    caps.supported_staging_file_formats = ["parquet"]
    caps.escape_identifier = escape_athena_identifier
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    caps.max_identifier_length = 255
    caps.max_column_identifier_length = 255
    caps.max_query_length = 16 * 1024 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 262144
    caps.is_max_text_data_type_length_in_bytes = True
    caps.supports_ddl_transactions = False
    caps.supports_transactions = False
    caps.alter_add_multi_column = True
    caps.schema_supports_numeric_precision = False
    caps.timestamp_precision = 3
    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.athena.athena import AthenaClient
    return AthenaClient(schema, _configure(initial_config))  # type: ignore


def spec() -> Type[DestinationClientConfiguration]:
    return AthenaClientConfiguration