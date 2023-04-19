from collections import defaultdict
from typing import Callable, Tuple, Iterable, Optional, Any, cast, List, Iterator, Dict, Union

from jsonpath_ng import parse as jsonpath_parse, JSONPath

from dlt.common.exceptions import TerminalException
from dlt.common.schema.utils import get_child_tables

from dlt.pipeline.exceptions import PipelineStepFailed, PipelineHasPendingDataException
from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline import Pipeline
from dlt.common.pipeline import TSourceState


def retry_load(retry_on_pipeline_steps: Tuple[TPipelineStep, ...] = ("load",)) -> Callable[[Exception], bool]:
    """A retry strategy for Tenacity that, with default setting, will repeat `load` step for all exceptions that are not terminal

    Use this condition with tenacity `retry_if_exception`. Terminal exceptions are exceptions that will not go away when operations is repeated.
    Examples: missing configuration values, Authentication Errors, terminally failed jobs exceptions etc.

    >>> data = source(...)
    >>> for attempt in Retrying(stop=stop_after_attempt(3), retry=retry_if_exception(retry_load(())), reraise=True):
    >>>     with attempt:
    >>>         p.run(data)

    Args:
        retry_on_pipeline_steps (Tuple[TPipelineStep, ...], optional): which pipeline steps are allowed to be repeated. Default: "load"

    """
    def _retry_load(ex: Exception) -> bool:
        # do not retry in normalize or extract stages
        if isinstance(ex, PipelineStepFailed) and ex.step not in retry_on_pipeline_steps:
            return False
        # do not retry on terminal exceptions
        if isinstance(ex, TerminalException) or (ex.__context__ is not None and isinstance(ex.__context__, TerminalException)):
            return False
        return True

    return _retry_load


class _DropCommand:
    def __init__(
        self,
        pipeline: Pipeline,
        resources: Optional[Union[Iterable[str], str]] = None,
        schema_name: str = None,
        state_paths: Optional[Union[Iterable[str], str]] = None
    ) -> None:
        self.pipeline = pipeline
        # self.tables = set(tables or [])
        if isinstance(resources, str):
            resources = [resources]
        if isinstance(state_paths, str):
            state_paths = [state_paths]
        resources = set(resources or [])
        self.schema = pipeline.schemas[schema_name or pipeline.default_schema_name].clone()
        self.schema_tables = self.schema.tables

        drop_tables = []
        for tbl in self.schema_tables.values():
            if tbl.get('resource') in resources:
                drop_tables += get_child_tables(self.schema_tables, tbl['name'])
        self.drop_tables = list(reversed(drop_tables))

        # # List resource state keys to drop
        # drop_resources = set(r for r in (t.get('resource') for t in self.drop_tables) if r)
        self.drop_state_paths: List[JSONPath] = [jsonpath_parse(p) for p in state_paths or []]
        self.drop_resource_state_keys = resources

    def drop_destination_tables(self) -> None:
        with self.pipeline._get_destination_client(self.schema) as client:
            client.drop_tables(*[tbl['name'] for tbl in self.drop_tables])

    def delete_pipeline_tables(self) -> None:
        for tbl in self.drop_tables:
            del self.schema_tables[tbl['name']]
        self.schema.bump_version()

    def _list_state_paths(self, source_state: Dict[str, Any]) -> Iterator[str]:
        for path in self.drop_state_paths:
            matches = path.find(source_state)
            for match in matches:
                yield str(match.full_path)

    def _drop_state_paths(self, source_state: Dict[str, Any]) -> None:
        for path in self.drop_state_paths:
            path.filter(lambda _: True, source_state)

    def drop_state_keys(self) -> None:
        state: TSourceState
        with self.pipeline.managed_state(extract_state=True, extract_unchanged=True) as state:  # type: ignore[assignment]
            source_states = state.get("sources")
            if not source_states:
                return
            # TODO: Should only drop from source state matching the schema. Not possible atm
            for source_state in source_states.values():
                for p in self.drop_state_paths:  # Remove keys by JSON path
                    p.filter(lambda _: True, source_state)
                resources = source_state.get('resources')
                if not resources:
                    return
                for key in self.drop_resource_state_keys:
                    resources.pop(key, None)

    def info(self) -> Any:
        return dict(
            drop_tables=[tbl['name'] for tbl in self.drop_tables],
        )

    def __call__(self) -> None:
        if self.pipeline.has_pending_data:  # Raise when there are pending extracted/load files to prevent conflicts
            raise PipelineHasPendingDataException(self.pipeline.pipeline_name, self.pipeline.pipelines_dir)
        self.delete_pipeline_tables()
        self.drop_destination_tables()
        self.drop_state_keys()
        self.pipeline.schemas.save_schema(self.schema)
        # Send updated state to destination
        self.pipeline.normalize()
        self.pipeline._get_load_storage().delete_completed_package
        try:
            self.pipeline.load(raise_on_failed_jobs=True)
        except Exception:
            # Clear extracted state on failure so command can run again
            self.pipeline._get_load_storage().wipe_normalized_packages()
            raise


def drop(
    pipeline: Pipeline,
    resources: Optional[Union[Iterable[str], str]] = None,
    schema_name: str = None,
    state_paths: Optional[Union[Iterable[str], str]] = None
) -> None:
    return _DropCommand(pipeline, resources, schema_name, state_paths)()
