import datetime  # noqa: I251
from typing import Any, Dict, List, NamedTuple, Optional, Tuple  # noqa: 251
from dlt.common.typing import TypedDict


class DataWriterMetrics(NamedTuple):
    file_path: str
    items_count: int
    file_size: int
    created: float
    last_modified: float

    def __add__(self, other: Tuple[object, ...], /) -> Tuple[object, ...]:  # type: ignore[override]
        if isinstance(other, DataWriterMetrics):
            return DataWriterMetrics(
                self.file_path if self.file_path == other.file_path else "",
                # self.table_name if self.table_name == other.table_name else "",
                self.items_count + other.items_count,
                self.file_size + other.file_size,
                min(self.created, other.created),
                max(self.last_modified, other.last_modified),
            )
        return NotImplemented


class DataWriterAndCustomMetrics(DataWriterMetrics):
    custom_metrics: Dict[str, Any]

    def __new__(
        cls,
        file_path: str,
        items_count: int,
        file_size: int,
        created: float,
        last_modified: float,
        custom_metrics: Dict[str, Any] = None,
    ) -> "DataWriterAndCustomMetrics":
        self = super(DataWriterAndCustomMetrics, cls).__new__(
            cls, file_path, items_count, file_size, created, last_modified
        )
        self.custom_metrics = custom_metrics or {}
        return self

    def _asdict(self) -> Dict[str, Any]:
        """Override _asdict to include custom_metrics in serialization"""
        result = super()._asdict()
        result["custom_metrics"] = self.custom_metrics
        return result


class StepMetrics(TypedDict):
    """Metrics for particular package processed in particular pipeline step"""

    started_at: datetime.datetime
    """Start of package processing"""
    finished_at: datetime.datetime
    """End of package processing"""


class ExtractDataInfo(TypedDict):
    name: str
    data_type: str


class ExtractMetrics(StepMetrics):
    schema_name: str
    job_metrics: Dict[str, DataWriterMetrics]
    """Metrics collected per job id during writing of job file"""
    table_metrics: Dict[str, DataWriterMetrics]
    """Job metrics aggregated by table"""
    resource_metrics: Dict[str, DataWriterAndCustomMetrics]
    """Job metrics aggregated by resource"""
    dag: List[Tuple[str, str]]
    """A resource dag where elements of the list are graph edges"""
    hints: Dict[str, Dict[str, Any]]
    """Hints passed to the resources"""


class NormalizeMetrics(StepMetrics):
    job_metrics: Dict[str, DataWriterMetrics]
    """Metrics collected per job id during writing of job file"""
    table_metrics: Dict[str, DataWriterMetrics]
    """Job metrics aggregated by table"""


class LoadJobMetrics(NamedTuple):
    job_id: str
    file_path: str
    table_name: str
    started_at: datetime.datetime
    finished_at: Optional[datetime.datetime]
    state: str
    remote_url: Optional[str]
    retry_count: int = 0


class LoadMetrics(StepMetrics):
    job_metrics: Dict[str, LoadJobMetrics]
