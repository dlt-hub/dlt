from dataclasses import dataclass, field, asdict
import datetime  # noqa: I251
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Mapping  # noqa: 251
from dlt.common.typing import TypedDict
from types import MappingProxyType


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


@dataclass(frozen=True)
class DataWriterAndCustomMetrics:
    writer_metrics: DataWriterMetrics
    _custom: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_writer_metrics(
        cls,
        writer_metrics: DataWriterMetrics,
        resource_metrics: Dict[str, Any] = None,
        step_metrics: Dict[str, Any] = None,
    ) -> "DataWriterAndCustomMetrics":
        custom = {}
        if resource_metrics:
            custom.update(resource_metrics)
        if step_metrics:
            custom.update(step_metrics)
        return cls(writer_metrics, custom)

    @property
    def file_path(self) -> str:
        return self.writer_metrics.file_path

    @property
    def items_count(self) -> int:
        return self.writer_metrics.items_count

    @property
    def file_size(self) -> int:
        return self.writer_metrics.file_size

    @property
    def created(self) -> float:
        return self.writer_metrics.created

    @property
    def last_modified(self) -> float:
        return self.writer_metrics.last_modified

    @property
    def custom_metrics(self) -> Mapping[str, Any]:
        return MappingProxyType(self._custom)

    def _asdict(self) -> Dict[str, Any]:
        d = self.writer_metrics._asdict()
        d.update(self._custom)
        return d


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
    finished_at: datetime.datetime
    state: Optional[str]
    remote_url: Optional[str]


class LoadMetrics(StepMetrics):
    job_metrics: Dict[str, LoadJobMetrics]
