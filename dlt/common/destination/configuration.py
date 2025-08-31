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
