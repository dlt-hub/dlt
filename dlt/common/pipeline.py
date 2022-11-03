import os
import tempfile
from typing import Any, Callable, ClassVar, Dict, List, NamedTuple, Protocol, Sequence, Tuple

from dlt.common.configuration.container import ContainerInjectableContext
from dlt.common.configuration import configspec
from dlt.common.destination import DestinationReference
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition


class LoadInfo(NamedTuple):
    """A tuple holding the information on recently loaded packages. Returned by pipeline run method"""
    destination_name: str
    destination_displayable_credentials: str
    dataset_name: str
    loads_ids: List[Tuple[str, bool]]
    failed_jobs: Dict[str, Sequence[Tuple[str, str]]]


class SupportsPipeline(Protocol):
    """A protocol with core pipeline operations that lets high level abstractions ie. sources to access pipeline methods and properties"""
    def run(
        self,
        data: Any = None,
        *,
        destination: DestinationReference = None,
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
    # TODO: declare unresolvable generic types that will be allowed by configspec
    _deferred_pipeline: Callable[[], SupportsPipeline]
    _pipeline: SupportsPipeline

    can_create_default: ClassVar[bool] = False

    def pipeline(self) -> SupportsPipeline:
        """Creates or returns exiting pipeline"""
        if not self._pipeline:
            # delayed pipeline creation
            self._pipeline = self._deferred_pipeline()
        return self._pipeline

    def activate(self, pipeline: SupportsPipeline) -> None:
        self._pipeline = pipeline

    def is_activated(self) -> bool:
        return self._pipeline is not None

    def __init__(self, deferred_pipeline: Callable[..., SupportsPipeline]) -> None:
        """Initialize the context with a function returning the Pipeline object to allow creation on first use"""
        self._deferred_pipeline = deferred_pipeline


def get_default_working_dir() -> str:
    """ Gets default working dir of the pipeline, which may be
        1. in user home directory ~/.dlt/pipelines/
        2. if current user is root in /var/dlt/pipelines
        3. if current user does not have a home directory in /tmp/dlt/pipelines
    """
    if os.geteuid() == 0:
        # we are root so use standard /var
        return os.path.join("/var", "dlt", "pipelines")

    home = os.path.expanduser("~")
    if home is None:
        # no home dir - use temp
        return os.path.join(tempfile.gettempdir(), "dlt", "pipelines")
    else:
        # if home directory is available use ~/.dlt/pipelines
        return os.path.join(home, ".dlt", "pipelines")
