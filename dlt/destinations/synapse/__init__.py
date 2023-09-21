from typing import Type

from dlt.common.schema.schema import Schema
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.accessors import config
from dlt.common.data_writers.escape import escape_postgres_identifier, escape_synapse_literal
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import JobClientBase, DestinationClientConfiguration
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.wei import EVM_DECIMAL_PRECISION

from dlt.destinations.synapse.configuration import SynapseClientConfiguration


@with_config(spec=SynapseClientConfiguration, sections=(known_sections.DESTINATION, "synapse",))
def _configure(config: SynapseClientConfiguration = config.value) -> SynapseClientConfiguration:
    return config


def capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.preferred_loader_file_format = "insert_values"
    caps.supported_loader_file_formats = ["insert_values"]
    #TODO: Add a batch_copy for azure synapse if needed for preferred_loader_file_format
    #caps.preferred_loader_file_format = "batch_copy"
    #caps.supported_loader_file_formats = ["batch_copy"]
    caps.preferred_staging_file_format = None
    caps.supported_staging_file_formats = []
    #TODO: Add a blob_storage for azure synapse if needed for preferred_staging_file_format
    #caps.preferred_staging_file_format = "blob_storage"
    #caps.supported_staging_file_formats = ["blob_storage","parquet"]
    caps.escape_identifier = escape_postgres_identifier
    caps.escape_literal = escape_synapse_literal
    caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
    caps.wei_precision = (DEFAULT_NUMERIC_PRECISION, 0)
    # https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-ver16&redirectedfrom=MSDN
    caps.max_identifier_length = 128
    caps.max_column_identifier_length = 128
    caps.max_query_length = 4 * 1024 * 64 * 1024
    caps.is_max_query_length_in_bytes = True
    caps.max_text_data_type_length = 4000
    caps.is_max_text_data_type_length_in_bytes = False
    caps.supports_ddl_transactions = True
    caps.max_rows_per_insert = 1000

    return caps


def client(schema: Schema, initial_config: DestinationClientConfiguration = config.value) -> JobClientBase:
    # import client when creating instance so capabilities and config specs can be accessed without dependencies installed
    from dlt.destinations.synapse.synapse import SynapseClient

    return SynapseClient(schema, _configure(initial_config))  # type: ignore[arg-type]


def spec() -> Type[DestinationClientConfiguration]:
    return SynapseClientConfiguration

