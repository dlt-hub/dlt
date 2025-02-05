from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.filesystem.factory import filesystem as _filesystem


class hive(_filesystem):
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.preferred_table_format = "hive"
        caps.supported_table_formats = ["hive"]
        caps.loader_file_format_selector = None
        caps.merge_strategies_selector = None
        return caps
