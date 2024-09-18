from abc import ABC, abstractmethod
import dataclasses
import os
import datetime  # noqa: 251
import humanize
import contextlib

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
    TypedDict,
    Mapping,
    Literal,
)
from typing_extensions import NotRequired

from dlt.common.configuration import configspec
from dlt.common.configuration import known_sections
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.paths import get_dlt_data_dir
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.destination import TDestinationReferenceArg, TDestination
from dlt.common.destination.exceptions import DestinationHasFailedJobs
from dlt.common.exceptions import PipelineStateNotAvailable, SourceSectionNotAvailable
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
    TColumnNames,
    TColumnSchema,
    TWriteDispositionConfig,
    TSchemaContract,
)
from dlt.common.source import get_current_pipe_name
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.storages.load_storage import LoadPackageInfo
from dlt.common.time import ensure_pendulum_datetime, precise_time
from dlt.common.typing import DictStrAny, REPattern, StrAny, SupportsHumanize
from dlt.common.jsonpath import delete_matches, TAnyJsonPath
from dlt.common.data_writers.writers import TLoaderFileFormat
from dlt.common.utils import RowCounts, merge_row_counts
from dlt.common.versioned_state import TVersionedState


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


class TPipelineLocalState(TypedDict, total=False):
    first_run: bool
    """Indicates a first run of the pipeline, where run ends with successful loading of data"""
    _last_extracted_at: datetime.datetime
    """Timestamp indicating when the state was synced with the destination."""
    _last_extracted_hash: str
    """Hash of state that was recently synced with destination"""


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
    destination: TDestination
    """The destination reference which is ModuleType. `destination.__name__` returns the name string"""
    dataset_name: str
    """Name of the dataset to which pipeline will be loaded to"""
    runtime_config: RunConfiguration
    """A configuration of runtime options like logging level and format and various tracing options"""
    working_dir: str
    """A working directory of the pipeline"""
    pipeline_salt: str
    """A configurable pipeline secret to be used as a salt or a seed for encryption key"""
    first_run: bool
    """Indicates a first run of the pipeline, where run ends with successful loading of the data"""

    @property
    def state(self) -> TPipelineState:
        """Returns dictionary with pipeline state"""

    @property
    def schemas(self) -> Mapping[str, Schema]:
        """Mapping of all pipeline schemas"""

    def set_local_state_val(self, key: str, value: Any) -> None:
        """Sets value in local state. Local state is not synchronized with destination."""

    def get_local_state_val(self, key: str) -> Any:
        """Gets value from local state. Local state is not synchronized with destination."""

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
    ) -> LoadInfo: ...

    def _set_context(self, is_active: bool) -> None:
        """Called when pipeline context activated or deactivate"""

    def _make_schema_with_default_name(self) -> Schema:
        """Make a schema from the pipeline name using the name normalizer. "_pipeline" suffix is removed if present"""


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
    _deferred_pipeline: Callable[[], SupportsPipeline] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )
    _pipeline: SupportsPipeline = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    can_create_default: ClassVar[bool] = True

    def pipeline(self) -> SupportsPipeline:
        """Creates or returns exiting pipeline"""
        if not self._pipeline:
            # delayed pipeline creation
            assert self._deferred_pipeline is not None, (
                "Deferred pipeline creation function not provided to PipelineContext. Are you"
                " calling dlt.pipeline() from another thread?"
            )
            self.activate(self._deferred_pipeline())
        return self._pipeline

    def activate(self, pipeline: SupportsPipeline) -> None:
        # do not activate currently active pipeline
        if pipeline == self._pipeline:
            return
        self.deactivate()
        pipeline._set_context(True)
        self._pipeline = pipeline

    def is_active(self) -> bool:
        return self._pipeline is not None

    def deactivate(self) -> None:
        if self.is_active():
            self._pipeline._set_context(False)
        self._pipeline = None

    def __init__(self, deferred_pipeline: Callable[..., SupportsPipeline] = None) -> None:
        """Initialize the context with a function returning the Pipeline object to allow creation on first use"""
        self._deferred_pipeline = deferred_pipeline


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


def _sources_state(pipeline_state_: Optional[TPipelineState] = None, /) -> DictStrAny:
    global _last_full_state

    if pipeline_state_ is None:
        state, _ = pipeline_state(Container())
    else:
        state = pipeline_state_
    if state is None:
        raise PipelineStateNotAvailable()

    sources_state_: DictStrAny = state.setdefault(known_sections.SOURCES, {})  # type: ignore

    # allow inspection of last returned full state
    _last_full_state = state
    return sources_state_


def source_state() -> DictStrAny:
    """Returns a dictionary with the source-scoped state. Source-scoped state may be shared across the resources of a particular source. Please avoid using source scoped state. Check
    the `resource_state` function for resource-scoped state that is visible within particular resource. Dlt state is preserved across pipeline runs and may be used to implement incremental loads.

    #### Note:
    The source state is a python dictionary-like object that is available within the `@dlt.source` and `@dlt.resource` decorated functions and may be read and written to.
    The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
    When using the state:
    * The source state is scoped to a particular source and will be stored under the source name in the pipeline state
    * It is possible to share state across many sources if they share a schema with the same name
    * Any JSON-serializable values can be written and the read from the state. `dlt` dumps and restores instances of Python bytes, DateTime, Date and Decimal types.
    * The state available in the source decorated function is read only and any changes will be discarded.
    * The state available in the resource decorated function is writable and written values will be available on the next pipeline run
    """
    global _last_full_state

    container = Container()

    # get the source name from the section context
    source_state_key: str = None
    with contextlib.suppress(ContextDefaultCannotBeCreated):
        sections_context = container[ConfigSectionContext]
        source_state_key = sections_context.source_state_key

    if not source_state_key:
        raise SourceSectionNotAvailable()

    try:
        state = _sources_state()
    except PipelineStateNotAvailable as e:
        # Reraise with source section
        raise PipelineStateNotAvailable(source_state_key) from e

    return state.setdefault(source_state_key, {})  # type: ignore[no-any-return]


_last_full_state: TPipelineState = None


def _delete_source_state_keys(
    key: TAnyJsonPath, source_state_: Optional[DictStrAny] = None, /
) -> None:
    """Remove one or more key from the source state.
    The `key` can be any number of keys and/or json paths to be removed.
    """
    state_ = source_state() if source_state_ is None else source_state_
    delete_matches(key, state_)


def resource_state(
    resource_name: str = None, source_state_: Optional[DictStrAny] = None, /
) -> DictStrAny:
    """Returns a dictionary with the resource-scoped state. Resource-scoped state is visible only to resource requesting the access. Dlt state is preserved across pipeline runs and may be used to implement incremental loads.

    Note that this function accepts the resource name as optional argument. There are rare cases when `dlt` is not able to resolve resource name due to requesting function
    working in different thread than the main. You'll need to pass the name explicitly when you request resource_state from async functions or functions decorated with @defer.

    Summary:
        The resource state is a python dictionary-like object that is available within the `@dlt.resource` decorated functions and may be read and written to.
        The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
        When using the state:
        * The resource state is scoped to a particular resource requesting it.
        * Any JSON-serializable values can be written and the read from the state. `dlt` dumps and restores instances of Python bytes, DateTime, Date and Decimal types.
        * The state available in the resource decorated function is writable and written values will be available on the next pipeline run

    Example:
        The most typical use case for the state is to implement incremental load.
        >>> @dlt.resource(write_disposition="append")
        >>> def players_games(chess_url, players, start_month=None, end_month=None):
        >>>     checked_archives = dlt.current.resource_state().setdefault("archives", [])
        >>>     archives = players_archives(chess_url, players)
        >>>     for url in archives:
        >>>         if url in checked_archives:
        >>>             print(f"skipping archive {url}")
        >>>             continue
        >>>         else:
        >>>             print(f"getting archive {url}")
        >>>             checked_archives.append(url)
        >>>         # get the filtered archive
        >>>         r = requests.get(url)
        >>>         r.raise_for_status()
        >>>         yield r.json().get("games", [])

    Here we store all the urls with game archives in the state and we skip loading them on next run. The archives are immutable. The state will grow with the coming months (and more players).
    Up to few thousand archives we should be good though.
    Args:
        resource_name (str, optional): forces to use state for a resource with this name. Defaults to None.
        source_state_ (Optional[DictStrAny], optional): Alternative source state. Defaults to None.

    Raises:
        ResourceNameNotAvailable: Raise if used outside of resource context or from a different thread than main

    Returns:
        DictStrAny: State dictionary
    """
    state_ = source_state() if source_state_ is None else source_state_
    # backtrace to find the shallowest resource
    if not resource_name:
        resource_name = get_current_pipe_name()
    return state_.setdefault("resources", {}).setdefault(resource_name, {})  # type: ignore


def reset_resource_state(resource_name: str, source_state_: Optional[DictStrAny] = None, /) -> None:
    """Resets the resource state with name `resource_name` by removing it from `source_state`

    Args:
        resource_name: The resource key to reset
        state: Optional source state dictionary to operate on. Use when working outside source context.
    """
    state_ = source_state() if source_state_ is None else source_state_
    if "resources" in state_ and resource_name in state_["resources"]:
        state_["resources"].pop(resource_name)


def _get_matching_sources(
    pattern: REPattern, pipeline_state: Optional[TPipelineState] = None, /
) -> List[str]:
    """Get all source names in state matching the regex pattern"""
    state_ = _sources_state(pipeline_state)
    return [key for key in state_ if pattern.match(key)]


def _get_matching_resources(
    pattern: REPattern, source_state_: Optional[DictStrAny] = None, /
) -> List[str]:
    """Get all resource names in state matching the regex pattern"""
    state_ = source_state() if source_state_ is None else source_state_
    if "resources" not in state_:
        return []
    return [key for key in state_["resources"] if pattern.match(key)]


def get_dlt_pipelines_dir() -> str:
    """Gets default directory where pipelines' data will be stored
    1. in user home directory ~/.dlt/pipelines/
    2. if current user is root in /var/dlt/pipelines
    3. if current user does not have a home directory in /tmp/dlt/pipelines
    """
    return os.path.join(get_dlt_data_dir(), "pipelines")


def get_dlt_repos_dir() -> str:
    """Gets default directory where command repositories will be stored"""
    return os.path.join(get_dlt_data_dir(), "repos")
