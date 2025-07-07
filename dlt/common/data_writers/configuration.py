import warnings
from dlt.common.warnings import Dlt100DeprecationWarning
from dlt.common.destination.configuration import (
    CsvQuoting,
    CsvFormatConfiguration,
    ParquetFormatConfiguration,
)

warnings.warn(
    "Please import format configuration from dlt.common.destination.configuration",
    Dlt100DeprecationWarning,
    stacklevel=2,
)

__all__ = ["CsvQuoting", "CsvFormatConfiguration", "ParquetFormatConfiguration"]
