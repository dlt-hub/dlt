from abc import ABC, abstractmethod
import dataclasses
import datetime  # noqa: 251
import humanize
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    Mapping,
    Literal,
)
from typing_extensions import NotRequired

from dlt.common.typing import TypedDict
from dlt.common.configuration import configspec
from dlt.common.configuration import known_sections
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.destination import TDestinationReferenceArg, AnyDestination
from dlt.common.destination.client import JobClientBase
from dlt.common.destination.exceptions import DestinationHasFailedJobs
from dlt.common.exceptions import (
    PipelineStateNotAvailable,
    SourceSectionNotAvailable,
    ResourceNameNotAvailable,
)
from dlt.common.metrics import (
    DataWriterMetrics,
    ExtractDataInfo,
    ExtractMetrics,
    LoadMetrics,
    NormalizeMetrics,
    StepMetrics,
)
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    TColumnSchema,
    TWriteDispositionConfig,
    TSchemaContract,
)
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.storages.load_storage import LoadPackageInfo
from dlt.common.time import ensure_pendulum_datetime, precise_time
from dlt.common.typing import DictStrAny, StrAny, SupportsHumanize, TColumnNames
from dlt.common.data_writers.writers import TLoaderFileFormat
from dlt.common.utils import RowCounts, merge_row_counts
from dlt.common.versioned_state import TVersionedState
from dlt.common.runtime.collector_base import Collector


# TRefreshMode = Literal["full", "replace"]
TRefreshMode = Literal["drop_sources", "drop_resources", "drop_data"]


class _StepInfo(NamedTuple):
    pipeline: "SupportsPipeline"
    loads_ids: List[str]
    """ids of the loaded packages"""
    load_packages: List[LoadPackageInfo]
    """Information on loaded packages"""
    first_run: bool
    started_at: datetime.datetime
    finished_at: datetime.datetime


TStepMetricsCo = TypeVar("TStepMetricsCo", bound=StepMetrics, covariant=True)


class StepInfo(SupportsHumanize, Generic[TStepMetricsCo]):
    pipeline: "SupportsPipeline"
    metrics: Dict[str, List[TStepMetricsCo]]
    """Metrics per load id. If many sources with the same name were extracted, there will be more than 1 element in the list"""
    loads_ids: List[str]
    """ids of the loaded packages"""
    load_packages: List[LoadPackageInfo]
    """Information on loaded packages"""
    first_run: bool

    @property
    def started_at(self) -> datetime.datetime:
        """Returns the earliest start date of all collected metrics"""
        if not self.metrics:
            return None
        try:
            return min(m["started_at"] for l_m in self.metrics.values() for m in l_m)
        except ValueError:
            return None

    @property
    def finished_at(self) -> datetime.datetime:
        """Returns the latest end date of all collected metrics"""
        if not self.metrics:
            return None
        try:
            return max(m["finished_at"] for l_m in self.metrics.values() for m in l_m)
        except ValueError:
            return None

    def asdict(self) -> DictStrAny:
        # to be mixed with NamedTuple
        step_info: DictStrAny = self._asdict()  # type: ignore
        step_info["pipeline"] = {"pipeline_name": self.pipeline.pipeline_name}
        step_info["load_packages"] = [package.asdict() for package in self.load_packages]
        if self.metrics:
            step_info["started_at"] = self.started_at
            step_info["finished_at"] = self.finished_at
            all_metrics = []
            for load_id, metrics in step_info["metrics"].items():
                for metric in metrics:
                    all_metrics.append({**dict(metric), "load_id": load_id})

            step_info["metrics"] = all_metrics
        return step_info

    def __str__(self) -> str:
        return self.asstr(verbosity=0)

    @staticmethod
    def _load_packages_asstr(load_packages: List[LoadPackageInfo], verbosity: int) -> str:
        msg: str = ""
        for load_package in load_packages:
            cstr = (
                load_package.state.upper()
                if load_package.completed_at
                else f"{load_package.state.upper()} and NOT YET LOADED to the destination"
            )
            # now enumerate all complete loads if we have any failed packages
            # complete but failed job will not raise any exceptions
            failed_jobs = load_package.jobs["failed_jobs"]
            jobs_str = "no failed jobs" if not failed_jobs else f"{len(failed_jobs)} FAILED job(s)!"
            msg += f"\nLoad package {load_package.load_id} is {cstr} and contains {jobs_str}"
            if verbosity > 0:
                for failed_job in failed_jobs:
                    msg += (
                        f"\n\t[{failed_job.job_file_info.job_id()}]: {failed_job.failed_message}\n"
                    )
            if verbosity > 1:
                msg += "\nPackage details:\n"
                msg += load_package.asstr() + "\n"
        return msg

    @staticmethod
    def writer_metrics_asdict(
        job_metrics: Dict[str, DataWriterMetrics], key_name: str = "job_id", extend: StrAny = None
    ) -> List[DictStrAny]:
        entities = []
        for entity_id, metrics in job_metrics.items():
            d = metrics._asdict()
            if extend:
                d.update(extend)
            d[key_name] = entity_id
            # add job-level info if known
            if metrics.file_path:
                d["table_name"] = ParsedLoadJobFileName.parse(metrics.file_path).table_name
            entities.append(d)
        return entities

    def _astuple(self) -> _StepInfo:
        return _StepInfo(
            self.pipeline,
            self.loads_ids,
            self.load_packages,
            self.first_run,
            self.started_at,
            self.finished_at,
        )


class _ExtractInfo(NamedTuple):
    """NamedTuple cannot be part of the derivation chain so we must re-declare all fields to use it as mixin later"""

    pipeline: "SupportsPipeline"
    metrics: Dict[str, List[ExtractMetrics]]
    extract_data_info: List[ExtractDataInfo]
    loads_ids: List[str]
    """ids of the loaded packages"""
    load_packages: List[LoadPackageInfo]
    """Information on loaded packages"""
    first_run: bool


class ExtractInfo(StepInfo[ExtractMetrics], _ExtractInfo):  # type: ignore[misc]
    """A tuple holding information on extracted data items. Returned by pipeline `extract` method."""

    def asdict(self) -> DictStrAny:
        """A dictionary representation of ExtractInfo that can be loaded with `dlt`"""
        d = super().asdict()
        d.pop("extract_data_info")
        # transform metrics
        d.pop("metrics")
        load_metrics: Dict[str, List[Any]] = {
            "job_metrics": [],
            "table_metrics": [],
            "resource_metrics": [],
            "dag": [],
            "hints": [],
        }
        for load_id, metrics_list in self.metrics.items():
            for idx, metrics in enumerate(metrics_list):
                extend = {"load_id": load_id, "extract_idx": idx}
                load_metrics["resource_metrics"].extend(
                    self.writer_metrics_asdict(
                        metrics["resource_metrics"], key_name="resource_name", extend=extend
                    )
                )
                load_metrics["dag"].extend(
                    [
                        {**extend, "parent_name": edge[0], "resource_name": edge[1]}
                        for edge in metrics["dag"]
                    ]
                )
                load_metrics["hints"].extend(
                    [
                        {**extend, "resource_name": name, **hints}
                        for name, hints in metrics["hints"].items()
                    ]
                )
                load_metrics["job_metrics"].extend(
                    self.writer_metrics_asdict(metrics["job_metrics"], extend=extend)
                )
                load_metrics["table_metrics"].extend(
                    self.writer_metrics_asdict(
                        metrics["table_metrics"], key_name="table_name", extend=extend
                    )
                )

        d.update(load_metrics)
        return d

    def asstr(self, verbosity: int = 0) -> str:
        return self._load_packages_asstr(self.load_packages, verbosity)


class _NormalizeInfo(NamedTuple):
    pipeline: "SupportsPipeline"
    metrics: Dict[str, List[NormalizeMetrics]]
    loads_ids: List[str]
    """ids of the loaded packages"""
    load_packages: List[LoadPackageInfo]
    """Information on loaded packages"""
    first_run: bool


class NormalizeInfo(StepInfo[NormalizeMetrics], _NormalizeInfo):  # type: ignore[misc]
    """A tuple holding information on normalized data items. Returned by pipeline `normalize` method."""

    @property
    def row_counts(self) -> RowCounts:
        if not self.metrics:
            return {}
        counts: RowCounts = {}
        for metrics in self.metrics.values():
            assert len(metrics) == 1, "Cannot deal with more than 1 normalize metric per load_id"
            merge_row_counts(
                counts, {t: m.items_count for t, m in metrics[0]["table_metrics"].items()}
            )
        return counts

    def asdict(self) -> DictStrAny:
        """A dictionary representation of NormalizeInfo that can be loaded with `dlt`"""
        d = super().asdict()
        # transform metrics
        d.pop("metrics")
        load_metrics: Dict[str, List[Any]] = {
            "job_metrics": [],
            "table_metrics": [],
        }
        for load_id, metrics_list in self.metrics.items():
            for idx, metrics in enumerate(metrics_list):
                extend = {"load_id": load_id, "extract_idx": idx}
                load_metrics["job_metrics"].extend(
                    self.writer_metrics_asdict(metrics["job_metrics"], extend=extend)
                )
                load_metrics["table_metrics"].extend(
                    self.writer_metrics_asdict(
                        metrics["table_metrics"], key_name="table_name", extend=extend
                    )
                )
        d.update(load_metrics)
        return d

    def asstr(self, verbosity: int = 0) -> str:
        if self.row_counts:
            msg = "Normalized data for the following tables:\n"
            for key, value in self.row_counts.items():
                msg += f"- {key}: {value} row(s)\n"
        else:
            msg = "No data found to normalize"
        msg += self._load_packages_asstr(self.load_packages, verbosity)
        return msg


class _LoadInfo(NamedTuple):
    pipeline: "SupportsPipeline"
    metrics: Dict[str, List[LoadMetrics]]
    destination_type: str
    destination_displayable_credentials: str
    destination_name: str
    environment: str
    staging_type: str
    staging_name: str
    staging_displayable_credentials: str
    destination_fingerprint: str
    dataset_name: str
    loads_ids: List[str]
    """ids of the loaded packages"""
    load_packages: List[LoadPackageInfo]
    """Information on loaded packages"""
    first_run: bool


class LoadInfo(StepInfo[LoadMetrics], _LoadInfo):  # type: ignore[misc]
    """A tuple holding the information on recently loaded packages. Returned by pipeline `run` and `load` methods"""

    def asdict(self) -> DictStrAny:
        """A dictionary representation of LoadInfo that can be loaded with `dlt`"""
        d = super().asdict()
        # transform metrics
        d.pop("metrics")
        load_metrics: Dict[str, List[Any]] = {"job_metrics": []}
        for load_id, metrics_list in self.metrics.items():
            # one set of metrics per package id
            assert len(metrics_list) == 1
            metrics = metrics_list[0]
            for job_metrics in metrics["job_metrics"].values():
                load_metrics["job_metrics"].append({"load_id": load_id, **job_metrics._asdict()})

        d.update(load_metrics)
        return d

    def asstr(self, verbosity: int = 0) -> str:
        msg = f"Pipeline {self.pipeline.pipeline_name} load step completed in "
        if self.started_at:
            elapsed = self.finished_at - self.started_at
            msg += humanize.precisedelta(elapsed)
        else:
            msg += "---"
        msg += (
            f"\n{len(self.loads_ids)} load package(s) were loaded to destination"
            f" {self.destination_name} and into dataset {self.dataset_name}\n"
        )
        if self.staging_name:
            msg += (
                f"The {self.staging_name} staging destination used"
                f" {self.staging_displayable_credentials} location to stage data\n"
            )

        msg += (
            f"The {self.destination_name} destination used"
            f" {self.destination_displayable_credentials} location to store data"
        )
        msg += self._load_packages_asstr(self.load_packages, verbosity)

        return msg

    @property
    def has_failed_jobs(self) -> bool:
        """Returns True if any of the load packages has a failed job."""
        for load_package in self.load_packages:
            if len(load_package.jobs["failed_jobs"]):
                return True
        return False

    def raise_on_failed_jobs(self) -> None:
        """Raises `DestinationHasFailedJobs` exception if any of the load packages has a failed job."""
        for load_package in self.load_packages:
            failed_jobs = load_package.jobs["failed_jobs"]
            if len(failed_jobs):
                raise DestinationHasFailedJobs(
                    self.destination_name, load_package.load_id, failed_jobs
                )

    def __str__(self) -> str:
        return self.asstr(verbosity=1)


TStepMetrics = TypeVar("TStepMetrics", bound=StepMetrics, covariant=False)
TStepInfo = TypeVar("TStepInfo", bound=StepInfo[StepMetrics])


class WithStepInfo(ABC, Generic[TStepMetrics, TStepInfo]):
    """Implemented by classes that generate StepInfo with metrics and package infos"""

    _current_load_id: str
    _load_id_metrics: Dict[str, List[TStepMetrics]]
    _current_load_started: float
    """Completed load ids metrics"""

    def __init__(self) -> None:
        self._load_id_metrics = {}
        self._current_load_id = None
        self._current_load_started = None

    def _step_info_start_load_id(self, load_id: str) -> None:
        self._current_load_id = load_id
        self._current_load_started = precise_time()
        self._load_id_metrics.setdefault(load_id, [])

    def _step_info_complete_load_id(self, load_id: str, metrics: TStepMetrics) -> None:
        assert self._current_load_id == load_id, (
            f"Current load id mismatch {self._current_load_id} != {load_id} when completing step"
            " info"
        )
        metrics["started_at"] = ensure_pendulum_datetime(self._current_load_started)
        metrics["finished_at"] = ensure_pendulum_datetime(precise_time())
        self._load_id_metrics[load_id].append(metrics)
        self._current_load_id = None
        self._current_load_started = None

    def _step_info_metrics(self, load_id: str) -> List[TStepMetrics]:
        return self._load_id_metrics[load_id]

    @property
    def current_load_id(self) -> str:
        """Returns currently processing load id"""
        return self._current_load_id

    @abstractmethod
    def get_step_info(
        self,
        pipeline: "SupportsPipeline",
    ) -> TStepInfo:
        """Returns and instance of StepInfo with metrics and package infos"""
        pass


class TLastRunContext(TypedDict, total=False):
    """Stores context from the last successful pipeline run or sync"""

    run_dir: str
    """Defines the context working directory, defaults to cwd()"""
    uri: str
    """Uniquely identifies the context"""
    local_dir: str
    """Defines data dir where local relative dirs and files are created, defaults to run_dir"""
    settings_dir: str
    """Defines where the current settings (secrets and configs) are located"""


class TPipelineLocalState(TypedDict, total=False):
    first_run: bool
    """Indicates a first run of the pipeline, where run ends with successful loading of data"""
    _last_extracted_at: datetime.datetime
    """Timestamp indicating when the state was synced with the destination."""
    _last_extracted_hash: str
    """Hash of state that was recently synced with destination"""
    initial_cwd: str
    """Run dir when pipeline was instantiated for a first time, defaults to cwd on OSS run context"""
    last_run_context: Optional[TLastRunContext]
    """Context from the last successful pipeline run or sync"""


class TPipelineState(TVersionedState, total=False):
    """Schema for a pipeline state that is stored within the pipeline working directory"""

    pipeline_name: str
    dataset_name: str
    default_schema_name: Optional[str]
    """Name of the first schema added to the pipeline to which all the resources without schemas will be added"""
    schema_names: Optional[List[str]]
    """All the schemas present within the pipeline working directory"""
    destination_name: Optional[str]
    destination_type: Optional[str]
    staging_name: Optional[str]
    staging_type: Optional[str]

    # properties starting with _ are not automatically applied to pipeline object when state is restored
    _local: TPipelineLocalState
    """A section of state that is not synchronized with the destination and does not participate in change merging and version control"""

    sources: NotRequired[Dict[str, Dict[str, Any]]]


class TSourceState(TPipelineState):
    sources: Dict[str, Dict[str, Any]]  # type: ignore[misc]


class SupportsPipeline(Protocol):
    """A protocol with core pipeline operations that lets high level abstractions ie. sources to access pipeline methods and properties"""

    pipeline_name: str
    """Name of the pipeline"""
    default_schema_name: str
    """Name of the default schema"""
    destination: AnyDestination
    """The destination reference which is ModuleType. `destination.__name__` returns the name string"""
    dataset_name: str
    """Name of the dataset to which pipeline will be loaded to"""
    runtime_config: RuntimeConfiguration
    """A configuration of runtime options like logging level and format and various tracing options"""
    working_dir: str
    """A working directory of the pipeline"""
    pipeline_salt: str
    """A configurable pipeline secret to be used as a salt or a seed for encryption key"""
    first_run: bool
    """Indicates a first run of the pipeline, where run ends with successful loading of the data"""
    last_run_context: TLastRunContext
    """Stores last "good" run context, where run ends with successful loading of the data"""
    collector: Collector
    """A collector that tracks the progress of the pipeline"""

    @property
    def state(self) -> TPipelineState:
        """Returns dictionary with current pipeline state

        Returns:
            TPipelineState: The current pipeline state
        """

    @property
    def schemas(self) -> Mapping[str, Schema]:
        """Returns all known schemas of the pipeline

        Returns:
            Mapping[str, Schema]: A mapping of schema names to their corresponding Schema objects
        """

    def get_local_state_val(self, key: str) -> Any:
        """Gets value from local state. Local state is not synchronized with destination.

        Args:
            key (str): The key to get the value from

        Returns:
            Any: The value from the local state
        """

    def destination_client(self, schema_name: str = None) -> JobClientBase:
        """Get the destination job client for the configured destination

        Args:
            schema_name (str, optional): The name of the schema to get the client for. Defaults to None.

        Returns:
            JobClientBase: The destination job client
        """

    def run(
        self,
        data: Any = None,
        *,
        destination: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: Sequence[TColumnSchema] = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
    ) -> LoadInfo:
        """Loads the data into the destination

        Args:
            data (Any): The data to load. Can be a DltSource, DltResource or any iterable of data items.
            destination (TDestinationReferenceArg, optional): The destination to load the data to.
            dataset_name (str, optional): The name of the dataset to load the data to.
            credentials (Any, optional): The credentials to use to load the data.
            table_name (str, optional): The name of the table to load the data to.
            write_disposition (TWriteDispositionConfig, optional): The write disposition to use to load the data.
            columns (Sequence[TColumnSchema], optional): A list of column hints.
            primary_key (TColumnNames, optional): A list of column names to be used as primary key.
            schema (Schema, optional): A schema to use to load the data. Defaults to the schema configured in the pipeline.
            loader_file_format (TLoaderFileFormat, optional): The file format to use to load the data. Defaults to preferred loader file format of the destination.
            schema_contract (TSchemaContract, optional): The schema contract to use to load the data.

        Returns:
            LoadInfo: Information on the load operation
        """

    def _set_context(self, is_active: bool) -> None:
        """Called when pipeline context activated or deactivate"""


class SupportsPipelineRun(Protocol):
    def __call__(
        self,
        *,
        destination: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
    ) -> LoadInfo: ...


@configspec
class PipelineContext(ContainerInjectableContext):
    _PIPELINE_FACTORY: ClassVar[Callable[[], SupportsPipeline]] = None
    """Maker of default pipeline, set at runtime"""
    _pipeline: SupportsPipeline = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    """Active pipeline"""
    _pipelines: Optional[List[SupportsPipeline]] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    """History of previously activated pipelines, disabled by default"""
    enable_activation_history: bool = False
    """When True, references to activated pipelines will be also stored"""

    can_create_default: ClassVar[bool] = True

    def pipeline(self) -> SupportsPipeline:
        """Creates or returns an active pipeline"""
        if not self._pipeline:
            # delayed pipeline creation
            assert PipelineContext._PIPELINE_FACTORY is not None, (
                "Deferred pipeline creation function not provided to PipelineContext. Are you"
                " calling dlt.pipeline() from another thread?"
            )
            self.activate(PipelineContext._PIPELINE_FACTORY())
        return self._pipeline

    def activate(self, pipeline: SupportsPipeline) -> None:
        """Activates `pipeline` and deactivates active one."""
        # do not activate currently active pipeline
        if pipeline == self._pipeline:
            return
        self.deactivate()
        # TODO: (1) compare run_context in pipeline with currently active context (via uri) and warn if they differ
        # TODO: (2) activate the right pipeline context. that requires that we should change pluggable run context
        #       to thread-affine or even contextvar that works in async pools
        pipeline._set_context(True)
        self._pipeline = pipeline
        # store activated pipelines
        # NOTE: this prevents them from being garbage collected
        # NOTE: this context has thread affinity so each thread has own history
        if self.enable_activation_history:
            if self._pipelines is None:
                self._pipelines = []
            if pipeline not in self._pipelines:
                self._pipelines.append(pipeline)

    def is_active(self) -> bool:
        return self._pipeline is not None

    def deactivate(self) -> None:
        if self.is_active():
            self._pipeline._set_context(False)
        self._pipeline = None

    def clear_activation_history(self) -> None:
        self._pipelines = []

    def activation_history(self) -> List[SupportsPipeline]:
        """Get list of pipelines that were activated"""
        if not self.enable_activation_history:
            raise RuntimeError("Activation history is not enabled")
        return self._pipelines or []

    @classmethod
    def cls__init__(self, deferred_pipeline: Callable[..., SupportsPipeline] = None) -> None:
        """Initialize the context with a function returning the Pipeline object to allow creation on first use"""
        self._PIPELINE_FACTORY = deferred_pipeline


def current_pipeline() -> SupportsPipeline:
    """Gets active pipeline context or None if not found"""
    proxy = Container()[PipelineContext]
    if not proxy.is_active():
        return None
    return proxy.pipeline()


@configspec
class StateInjectableContext(ContainerInjectableContext):
    state: TPipelineState = None

    can_create_default: ClassVar[bool] = False


def pipeline_state(
    container: Container, initial_default: TPipelineState = None
) -> Tuple[TPipelineState, bool]:
    """Gets value of the state from context or active pipeline, if none found returns `initial_default`

    Injected state is called "writable": it is injected by the `Pipeline` class and all the changes will be persisted.
    The state coming from pipeline context or `initial_default` is called "read only" and all the changes to it will be discarded

    Returns tuple (state, writable)
    """
    try:
        # get injected state if present. injected state is typically "managed" so changes will be persisted
        return container[StateInjectableContext].state, True
    except ContextDefaultCannotBeCreated:
        # check if there's pipeline context
        proxy = container[PipelineContext]
        if not proxy.is_active():
            return initial_default, False
        else:
            # get unmanaged state that is read only
            # TODO: make sure that state if up to date by syncing the pipeline earlier
            return proxy.pipeline().state, False


def get_dlt_pipelines_dir() -> str:
    """Gets default directory where pipelines' data will be stored
    1. in user home directory ~/.dlt/pipelines/
    2. if current user is root in /var/dlt/pipelines
    3. if current user does not have a home directory in /tmp/dlt/pipelines
    """
    from dlt.common.runtime import run_context

    return run_context.active().get_data_entity("pipelines")


def get_dlt_repos_dir() -> str:
    """Gets default directory where command repositories will be stored"""
    from dlt.common.runtime import run_context

    return run_context.active().get_data_entity("repos")
