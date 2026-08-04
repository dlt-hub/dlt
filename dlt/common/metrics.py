import datetime  # noqa: I251
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)  # noqa: 251
from dlt.common.typing import NotRequired, TypedDict
from dlt.common.utils import digest128

TJobKey = TypeVar("TJobKey")


class DataWriterMetrics(NamedTuple):
    file_path: str
    items_count: int
    file_size: int
    created: float
    last_modified: float

    def __add__(self, other: Tuple[object, ...], /) -> "DataWriterMetrics":
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


EMPTY_DATA_WRITER_METRICS = DataWriterMetrics("", 0, 0, 2**32, 0.0)


def aggregate_job_metrics(
    job_metrics: Mapping[TJobKey, DataWriterMetrics],
    key: Callable[[TJobKey], str],
) -> Dict[str, DataWriterMetrics]:
    """Sum writer metrics grouped by `key`, independent of input order."""
    result: Dict[str, DataWriterMetrics] = {}
    for job_key, metrics in job_metrics.items():
        group_key = key(job_key)
        result[group_key] = result.get(group_key, EMPTY_DATA_WRITER_METRICS) + metrics
    return result


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
        """Includes custom_metrics in serialization, promoting list-valued
        metrics to top-level keys for cleaner child table names."""
        result = super()._asdict()
        standard_keys = set(result)
        nested: Dict[str, Any] = {}
        for key, value in self.custom_metrics.items():
            # skip list metrics that collide with standard NamedTuple fields
            if isinstance(value, list) and key not in standard_keys:
                result[key] = value
            else:
                nested[key] = value
        if nested:
            result["custom_metrics"] = nested
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


class TDataLocation(TypedDict):
    """A logical data source or target: a dataset of tables, an API of endpoints, a bucket of files.

    One entry per location. What a resource touched inside it is listed by the `kind`-specific
    subclass. Only facts that some locations genuinely lack are not required, so a row keeps the
    same shape whatever it describes.
    """

    kind: str
    """Location type: `dataset`, `sql_database`, `filesystem`, `rest_api`, or a custom value."""
    resource_name: str
    """Resource that read from or wrote to this location, authoritative when metrics are collected."""
    location: NotRequired[str]
    """Non-secret scope of the location, e.g. `postgresql://example.com:5432`, `s3://bucket`.

    The key is absent when the location has no public address, for example a reverse ETL sink.
    """
    version: NotRequired[str]
    """Version of the location's contents as a whole."""


class TSchemaReference(TypedDict):
    name: str
    version_hash: str


class TDatasetDataLocation(TDataLocation):
    """A dlt dataset: one or more schemas, holding tables, in a destination."""

    schemas: List[TSchemaReference]
    """Schemas grouping the tables, each carrying its own version hash."""
    tables: List[str]
    """Tables touched, as plain names - dlt qualifies tables by dataset, not by schema."""
    destination_type: str
    destination_name: str
    destination_fingerprint: str
    """Identifies destinations whose `location` is not public, ie. motherduck. May be empty."""
    casefold: str
    """Name of the casefolding function the destination applies: `upper`, `lower` or `str`."""
    case_sensitive: bool
    """Whether the destination generates case sensitive identifiers, as adjusted at runtime."""
    dataset_name: NotRequired[str]
    """Logical dataset name as configured by the user, e.g. `My_DataSet`. Absent for sinks."""
    physical_dataset_name: NotRequired[str]
    """Normalized name as it exists in the store, e.g. `my_data_set`. Absent for sinks."""


def data_location_version(schemas: Sequence[TSchemaReference]) -> str:
    """Hashes the version hashes of `schemas` into a single version of the location contents."""
    return digest128("".join(sorted(schema.get("version_hash") or "" for schema in schemas)))


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
    inputs: List[TDataLocation]
    """Locations read from, one entry per (resource, location)"""


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
    followup_jobs: Optional[Sequence[str]] = ()


class LoadMetrics(StepMetrics):
    job_metrics: Dict[str, LoadJobMetrics]
    dataset_name: Optional[str]
    """Physical dataset name, normalized as it exists in the destination"""
    outputs: List[TDatasetDataLocation]
    """Locations written to, one entry per (resource, location)"""
