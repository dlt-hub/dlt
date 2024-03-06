from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_writers import TLoaderFileFormat


def capabilities(
    preferred_loader_file_format: TLoaderFileFormat = "puae-jsonl",
    naming_convention: str = "direct",
) -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext.generic_capabilities(preferred_loader_file_format)
    caps.supported_loader_file_formats = ["puae-jsonl", "parquet"]
    caps.supports_ddl_transactions = False
    caps.supports_transactions = False
    caps.naming_convention = naming_convention
    return caps
