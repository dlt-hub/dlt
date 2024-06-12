from typing import Optional
from dlt.common.destination import DestinationCapabilitiesContext, TLoaderFileFormat
from dlt.common.destination.capabilities import TLoaderParallelismStrategy


def capabilities(
    preferred_loader_file_format: TLoaderFileFormat = "typed-jsonl",
    naming_convention: str = "direct",
    max_table_nesting: Optional[int] = 0,
    max_parallel_load_jobs: Optional[int] = 0,
    loader_parallelism_strategy: Optional[TLoaderParallelismStrategy] = None,
) -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext.generic_capabilities(preferred_loader_file_format)
    caps.supported_loader_file_formats = ["typed-jsonl", "parquet"]
    caps.supports_ddl_transactions = False
    caps.supports_transactions = False
    caps.naming_convention = naming_convention
    caps.max_table_nesting = max_table_nesting
    return caps
