import os
import datetime  # noqa: 251
import humanize
import contextlib
from typing import Any, Callable, ClassVar, Dict, List, NamedTuple, Optional, Protocol, Sequence, TYPE_CHECKING, Tuple, TypedDict
from typing_extensions import NotRequired

from dlt.common import pendulum, logger
from dlt.common.configuration import configspec
from dlt.common.configuration import known_sections
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.configuration.paths import get_dlt_data_dir
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.destination import DestinationReference, TDestinationReferenceArg
from dlt.common.exceptions import DestinationHasFailedJobs, PipelineStateNotAvailable, ResourceNameNotAvailable, SourceSectionNotAvailable
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnNames, TColumnSchema, TWriteDisposition
from dlt.common.source import get_current_pipe_name
from dlt.common.storages.load_storage import LoadPackageInfo
from dlt.common.typing import DictStrAny, REPattern
from dlt.common.jsonpath import delete_matches, TAnyJsonPath
from dlt.common.data_writers.writers import TLoaderFileFormat


class ExtractDataInfo(TypedDict):
    name: str
    data_type: str


class ExtractInfo(NamedTuple):
    """A tuple holding information on extracted data items. Returned by pipeline `extract` method."""

    extract_data_info: List[ExtractDataInfo]

    def asdict(self) -> DictStrAny:
        return {}

    def asstr(self, verbosity: int = 0) -> str:
        return ""

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class NormalizeInfo(NamedTuple):
    """A tuple holding information on normalized data items. Returned by pipeline `normalize` method."""

    row_counts: Dict[str, int] = {}

    def asdict(self) -> DictStrAny:
        """A dictionary representation of NormalizeInfo that can be loaded with `dlt`"""
        d = self._asdict()
        # list representation creates a nice table
        d["row_counts"] = [(k, v) for k, v in self.row_counts.items()]
        return d

    def asstr(self, verbosity: int = 0) -> str:
        if self.row_counts:
            msg = "Normalized data for the following tables:\n"
            for key, value in self.row_counts.items():
                msg += f"- {key}: {value} row(s)\n"
        else:
            msg = "No data found to normalize"
        return msg

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class LoadInfo(NamedTuple):
    """A tuple holding the information on recently loaded packages. Returned by pipeline `run` and `load` methods"""
    pipeline: "SupportsPipeline"
    destination_name: str
    destination_displayable_credentials: str
    staging_name: str
    staging_displayable_credentials: str
    destination_fingerprint: str
    dataset_name: str
    loads_ids: List[str]
    """ids of the loaded packages"""
    load_packages: List[LoadPackageInfo]
    """Information on loaded packages"""
    started_at: datetime.datetime
    first_run: bool

    def asdict(self) -> DictStrAny:
        """A dictionary representation of LoadInfo that can be loaded with `dlt`"""
        d = self._asdict()
        d["pipeline"] = {
            "pipeline_name": self.pipeline.pipeline_name
        }
        d["load_packages"] = [package.asdict() for package in self.load_packages]
        return d

    def asstr(self, verbosity: int = 0) -> str:
        msg = f"Pipeline {self.pipeline.pipeline_name} completed in "
        if self.started_at:
            elapsed = pendulum.now() - self.started_at
            msg += humanize.precisedelta(elapsed)
        else:
            msg += "---"
        msg += f"\n{len(self.loads_ids)} load package(s) were loaded to destination {self.destination_name} and into dataset {self.dataset_name}\n"
        if self.staging_name:
            msg += f"The {self.staging_name} staging destination used {self.staging_displayable_credentials} location to stage data\n"

        msg += f"The {self.destination_name} destination used {self.destination_displayable_credentials} location to store data"
        for load_package in self.load_packages:
            cstr = load_package.state.upper() if load_package.completed_at else "NOT COMPLETED"
            # now enumerate all complete loads if we have any failed packages
            # complete but failed job will not raise any exceptions
            failed_jobs = load_package.jobs["failed_jobs"]
            jobs_str = "no failed jobs" if not failed_jobs else f"{len(failed_jobs)} FAILED job(s)!"
            msg += f"\nLoad package {load_package.load_id} is {cstr} and contains {jobs_str}"
            if verbosity > 0:
                for failed_job in failed_jobs:
                    msg += f"\n\t[{failed_job.job_file_info.job_id()}]: {failed_job.failed_message}\n"
            if verbosity > 1:
                msg += "\nPackage details:\n"
                msg += load_package.asstr() + "\n"
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
                raise DestinationHasFailedJobs(self.destination_name, load_package.load_id, failed_jobs)

    def __str__(self) -> str:
        return self.asstr(verbosity=1)

class TPipelineLocalState(TypedDict, total=False):
    first_run: bool
    """Indicates a first run of the pipeline, where run ends with successful loading of data"""
    _last_extracted_at: datetime.datetime
    """Timestamp indicating when the state was synced with the destination. Lack of timestamp means not synced state."""


class TPipelineState(TypedDict, total=False):
    """Schema for a pipeline state that is stored within the pipeline working directory"""
    pipeline_name: str
    dataset_name: str
    default_schema_name: Optional[str]
    """Name of the first schema added to the pipeline to which all the resources without schemas will be added"""
    schema_names: Optional[List[str]]
    """All the schemas present within the pipeline working directory"""
    destination: Optional[str]
    staging: Optional[str]

    # properties starting with _ are not automatically applied to pipeline object when state is restored
    _state_version: int
    _state_engine_version: int
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
    destination: DestinationReference
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
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None
        ) -> LoadInfo:
        ...

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
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None
    ) -> LoadInfo:
        ...


@configspec
class PipelineContext(ContainerInjectableContext):
    _deferred_pipeline: Callable[[], SupportsPipeline]
    _pipeline: SupportsPipeline

    can_create_default: ClassVar[bool] = False

    def pipeline(self) -> SupportsPipeline:
        """Creates or returns exiting pipeline"""
        if not self._pipeline:
            # delayed pipeline creation
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

    def __init__(self, deferred_pipeline: Callable[..., SupportsPipeline]) -> None:
        """Initialize the context with a function returning the Pipeline object to allow creation on first use"""
        self._deferred_pipeline = deferred_pipeline


@configspec
class StateInjectableContext(ContainerInjectableContext):
    state: TPipelineState

    can_create_default: ClassVar[bool] = False

    if TYPE_CHECKING:
        def __init__(self, state: TPipelineState = None) -> None:
            ...


def pipeline_state(container: Container, initial_default: TPipelineState = None) -> Tuple[TPipelineState, bool]:
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


def _delete_source_state_keys(key: TAnyJsonPath, source_state_: Optional[DictStrAny] = None, /) -> None:
    """Remove one or more key from the source state.
    The `key` can be any number of keys and/or json paths to be removed.
    """
    state_ = source_state() if source_state_ is None else source_state_
    delete_matches(key, state_)


def resource_state(resource_name: str = None, source_state_: Optional[DictStrAny] = None, /) -> DictStrAny:
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
    if not resource_name:
        raise ResourceNameNotAvailable()
    return state_.setdefault('resources', {}).setdefault(resource_name, {})  # type: ignore


def reset_resource_state(resource_name: str, source_state_: Optional[DictStrAny] = None, /) -> None:
    """Resets the resource state with name `resource_name` by removing it from `source_state`

    Args:
        resource_name: The resource key to reset
        state: Optional source state dictionary to operate on. Use when working outside source context.
    """
    state_ = source_state() if source_state_ is None else source_state_
    if "resources" in state_ and resource_name in state_["resources"]:
        state_["resources"].pop(resource_name)


def _get_matching_resources(pattern: REPattern, source_state_: Optional[DictStrAny] = None, /) -> List[str]:
    """Get all resource names in state matching the regex pattern"""
    state_ = source_state() if source_state_ is None else source_state_
    if "resources" not in state_:
        return []
    return [key for key in state_["resources"] if pattern.match(key)]


def get_dlt_pipelines_dir() -> str:
    """ Gets default directory where pipelines' data will be stored
        1. in user home directory ~/.dlt/pipelines/
        2. if current user is root in /var/dlt/pipelines
        3. if current user does not have a home directory in /tmp/dlt/pipelines
    """
    return os.path.join(get_dlt_data_dir(), "pipelines")


def get_dlt_repos_dir() -> str:
    """Gets default directory where command repositories will be stored"""
    return os.path.join(get_dlt_data_dir(), "repos")
