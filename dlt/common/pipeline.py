import os
import datetime  # noqa: 251
import humanize
from typing import Any, Callable, ClassVar, Dict, List, NamedTuple, Optional, Protocol, Sequence, Tuple, TypedDict

from dlt.common import pendulum
from dlt.common.configuration import configspec
from dlt.common.configuration.container import ContainerInjectableContext
from dlt.common.configuration.paths import get_dlt_home_dir
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.destination.reference import DestinationReference, TDestinationReferenceArg
from dlt.common.exceptions import DestinationHasFailedJobs
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition
from dlt.common.storages.load_storage import LoadPackageInfo
from dlt.common.typing import DictStrAny


class ExtractInfo(NamedTuple):
    """A tuple holding information on extracted data items. Returned by pipeline `extract` method."""
    def asdict(self) -> DictStrAny:
        return {}

    def asstr(self, verbosity: int = 0) -> str:
        return ""

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class NormalizeInfo(NamedTuple):
    """A tuple holding information on normalized data items. Returned by pipeline `normalize` method."""
    def asdict(self) -> DictStrAny:
        return {}

    def asstr(self, verbosity: int = 0) -> str:
        return ""

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class LoadInfo(NamedTuple):
    """A tuple holding the information on recently loaded packages. Returned by pipeline `run` and `load` methods"""
    pipeline: "SupportsPipeline"
    destination_name: str
    destination_displayable_credentials: str
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
            if len(load_package.jobs["failed_jobs"]):
                raise DestinationHasFailedJobs(self.destination_name, load_package.load_id)

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

    # properties starting with _ are not automatically applied to pipeline object when state is restored
    _state_version: int
    _state_engine_version: int
    _local: TPipelineLocalState
    """A section of state that is not synchronized with the destination and does not participate in change merging and version control"""


class SupportsPipeline(Protocol):
    """A protocol with core pipeline operations that lets high level abstractions ie. sources to access pipeline methods and properties"""
    pipeline_name: str
    """Name of the pipeline"""
    destination: DestinationReference
    """The destination reference which is ModuleType. `destination.__name__` returns the name string"""
    dataset_name: str = None
    """Name of the dataset to which pipeline will be loaded to"""
    runtime_config: RunConfiguration
    """A configuration of runtime options like logging level and format and various tracing options"""
    working_dir: str
    """A working directory of the pipeline"""

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
        schema: Schema = None
    ) -> LoadInfo:
        ...

    def _set_context(self, is_active: bool) -> None:
        """Called when pipeline context activated or deactivate"""
        ...


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
        schema: Schema = None
    ) -> LoadInfo:
        ...


@configspec(init=True)
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


def get_dlt_pipelines_dir() -> str:
    """ Gets default directory where pipelines' data will be stored
        1. in user home directory ~/.dlt/pipelines/
        2. if current user is root in /var/dlt/pipelines
        3. if current user does not have a home directory in /tmp/dlt/pipelines
    """
    return os.path.join(get_dlt_home_dir(), "pipelines")


def get_dlt_repos_dir() -> str:
    """Gets default directory where command repositories will be stored"""
    return os.path.join(get_dlt_home_dir(), "repos")
