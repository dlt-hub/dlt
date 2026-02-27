from typing import ClassVar, Literal, Optional

from dlt.common.configuration import configspec, known_sections
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.time import get_precision_from_datetime_unit

CsvQuoting = Literal["quote_all", "quote_needed", "quote_minimal", "quote_none"]


@configspec
class CsvFormatConfiguration(BaseConfiguration):
    delimiter: str = ","
    include_header: bool = True
    quoting: CsvQuoting = "quote_needed"
    lineterminator: str = "\n"

    # read options
    on_error_continue: bool = False
    encoding: str = "utf-8"

    __section__: ClassVar[str] = known_sections.DATA_WRITER


@configspec
class ParquetFormatConfiguration(BaseConfiguration):
    flavor: Optional[str] = None  # could be ie. "spark"
    version: Optional[str] = "2.4"
    data_page_size: Optional[int] = None
    timestamp_timezone: str = "UTC"
    row_group_size: Optional[int] = None
    coerce_timestamps: Optional[Literal["s", "ms", "us", "ns"]] = None
    allow_truncated_timestamps: bool = False
    use_compliant_nested_type: bool = True
    write_page_index: bool = False
    use_content_defined_chunking: bool = False  # requires pyarrow>=21.0.0, ignored otherwise
    supports_dictionary_encoding: bool = True
    """When False, constant columns (like _dlt_load_id) will use regular arrays instead of
    dictionary-encoded arrays. Set to False for destinations using ADBC drivers that don't
    support dictionary types (e.g., MSSQL)."""

    def max_timestamp_precision(self) -> int:
        if (self.flavor or "").lower() == "spark":
            base = get_precision_from_datetime_unit("ns")  # INT96 → treat as ns-capable
        else:
            v = float(self.version or "0.0")
            base = (
                get_precision_from_datetime_unit("ns")
                if v >= 2.6
                else get_precision_from_datetime_unit("us")
            )

        if self.coerce_timestamps:
            return min(base, get_precision_from_datetime_unit(self.coerce_timestamps))
        return base

    __section__: ClassVar[str] = known_sections.DATA_WRITER


@configspec
class ArrowIPCFormatConfiguration(BaseConfiguration):
    """Apache Arrow IPC Feather v2 format configuration

    This configuration provides the pyarrow.ipc.IpcWriteOptions settings for the Arrow IPC writer.
    Note: Legacy attributes have not been included.

    Attributes:
        allow_64bit: allow field lengths that do not fit in a signed 32-bit int
        compression: compression codec to use for record batch buffers ("lz4", "zstd", or None)
        use_threads: whether to use the global CPU thread pool to parallelize any computational tasks
        emit_dictionary_deltas: whether to emit dictionary deltas
        unify_dictionaries: will attempt to unify dictionaries across all batches in the table
    """

    allow_64bit: bool = False
    compression: Optional[Literal["lz4", "zstd"]] = None
    use_threads: bool = True
    emit_dictionary_deltas: bool = False  # default is false for maximum stream compatibility.
    unify_dictionaries: bool = False

    __section__: ClassVar[str] = known_sections.DATA_WRITER
