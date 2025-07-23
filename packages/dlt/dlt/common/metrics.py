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
    resource_metrics: Dict[str, DataWriterMetrics]
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
    finished_at: datetime.datetime
    state: Optional[str]
    remote_url: Optional[str]


class LoadMetrics(StepMetrics):
    job_metrics: Dict[str, LoadJobMetrics]
