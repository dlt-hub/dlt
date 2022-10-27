import os
import tempfile
from typing import Any, Callable, ClassVar, Protocol, Sequence

from dlt.common.configuration.container import ContainerInjectableContext
from dlt.common.configuration import configspec
from dlt.common.destination import DestinationReference
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition


class SupportsPipeline(Protocol):
    """A protocol with core pipeline operations that lets high level abstractions ie. sources to access pipeline methods and properties"""
    def run(
        self,
        source: Any = None,
        destination: DestinationReference = None,
        dataset_name: str = None,
        table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None
    ) -> Any:
        ...


@configspec(init=True)
class PipelineContext(ContainerInjectableContext):
    _deferred_pipeline: Any
    _pipeline: Any

    can_create_default: ClassVar[bool] = False

    def pipeline(self) -> SupportsPipeline:
        if not self._pipeline:
            # delayed pipeline creation
            self._pipeline = self._deferred_pipeline()
        return self._pipeline

    def activate(self, pipeline: SupportsPipeline) -> None:
        self._pipeline = pipeline

    def is_activated(self) -> bool:
        return self._pipeline is not None

    def __init__(self, deferred_pipeline: Callable[..., SupportsPipeline]) -> None:
        self._deferred_pipeline = deferred_pipeline


def get_default_working_dir() -> str:
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
