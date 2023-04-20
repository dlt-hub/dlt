from collections import defaultdict
from typing import Callable, Tuple, Iterable, Optional, Any, cast, List, Iterator, Dict, Union
from itertools import chain

# from jsonpath_ng import parse as jsonpath_parse, JSONPath
from dlt.common.jsonpath import resolve_paths, TAnyJsonPath, compile_paths

from dlt.common.exceptions import TerminalException
from dlt.common.schema.utils import get_child_tables, group_tables_by_resource, compile_simple_regexes
from dlt.common.schema.typing import TSimpleRegex
from dlt.common.typing import REPattern

from dlt.pipeline.exceptions import PipelineStepFailed, PipelineHasPendingDataException
from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline import Pipeline
from dlt.common.pipeline import TSourceState, _reset_resource_state, sources_state, _delete_source_state_keys, _get_matching_resources


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
        resources: Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]] = (),
        schema_name: Optional[str] = None,
        state_paths: TAnyJsonPath = ()
    ) -> None:
        self.pipeline = pipeline
        if isinstance(resources, str):
            resources = [resources]
        if isinstance(state_paths, str):
            state_paths = [state_paths]

        self.schema = pipeline.schemas[schema_name or pipeline.default_schema_name].clone()
        self.schema_tables = self.schema.tables

        resources = set(resources)
        if resources:
            self.resource_pattern = compile_simple_regexes(TSimpleRegex(r) for r in resources)
            resource_tables = group_tables_by_resource(self.schema_tables, pattern=self.resource_pattern)
            self.drop_tables = list(chain.from_iterable(resource_tables.values()))
            self.drop_tables.reverse()
        else:
            self.resource_pattern = None
            self.drop_tables = []

        self.drop_state_paths = compile_paths(state_paths)

    def drop_destination_tables(self) -> None:
        with self.pipeline._get_destination_client(self.schema) as client:
            client.drop_tables(*[tbl['name'] for tbl in self.drop_tables])

    def delete_pipeline_tables(self) -> None:
        for tbl in self.drop_tables:
            del self.schema_tables[tbl['name']]
        self.schema.bump_version()

    def _list_state_paths(self, source_state: Dict[str, Any]) -> List[str]:
        return resolve_paths(self.drop_state_paths, source_state)

    def drop_state_keys(self) -> None:
        state: TSourceState
        with self.pipeline.managed_state(extract_state=True, extract_unchanged=True) as state:  # type: ignore[assignment]
            source_states = sources_state(state).values()
            # TODO: Should only drop from source state matching the schema. Not possible atm
            for source_state in source_states:
                for key in _get_matching_resources(self.resource_pattern, source_state):
                    _reset_resource_state(key, source_state)
                _delete_source_state_keys(self.drop_state_paths, source_state)

    def info(self) -> Any:
        return dict(
            drop_tables=[tbl['name'] for tbl in self.drop_tables],
        )

    def __call__(self) -> None:
        if self.pipeline.has_pending_data:  # Raise when there are pending extracted/load files to prevent conflicts
            raise PipelineHasPendingDataException(self.pipeline.pipeline_name, self.pipeline.pipelines_dir)
        if self.resource_pattern is None and not self.drop_state_paths:
            return  # TODO: Nothing to drop, return info

        self.delete_pipeline_tables()
        self.drop_destination_tables()
        self.drop_state_keys()
        self.pipeline.schemas.save_schema(self.schema)
        # Send updated state to destination
        self.pipeline.normalize()
        try:
            self.pipeline.load(raise_on_failed_jobs=True)
        except Exception:
            # Clear extracted state on failure so command can run again
            self.pipeline._get_load_storage().wipe_normalized_packages()
            raise
        # TODO: Return info


def drop(
    pipeline: Pipeline,
    resources: Union[Iterable[str], str] = (),
    schema_name: str = None,
    state_paths: TAnyJsonPath = ()
) -> None:
    return _DropCommand(pipeline, resources, schema_name, state_paths)()
