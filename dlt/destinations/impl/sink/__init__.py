from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_writers import TLoaderFileFormat


def capabilities(
    preferred_loader_file_format: TLoaderFileFormat = "parquet",
) -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext.generic_capabilities(preferred_loader_file_format)
    caps.supports_ddl_transactions = False
    caps.supports_transactions = False
    return caps
