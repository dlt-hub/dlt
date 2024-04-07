import contextlib
from typing import Callable, Sequence, Iterable, Optional, Any, List, Dict, Union, TypedDict
from itertools import chain

from dlt.common.jsonpath import resolve_paths, TAnyJsonPath, compile_paths
from dlt.common.exceptions import TerminalException
from dlt.common.schema.utils import (
    group_tables_by_resource,
    compile_simple_regexes,
    compile_simple_regex,
)
from dlt.common.schema.typing import TSimpleRegex
from dlt.common.typing import REPattern
from dlt.common.pipeline import (
    reset_resource_state,
    _sources_state,
    _delete_source_state_keys,
    _get_matching_resources,
)
from dlt.common.destination.reference import WithStagingDataset

from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.pipeline.exceptions import (
    PipelineNeverRan,
    PipelineStepFailed,
    PipelineHasPendingDataException,
)
from dlt.pipeline.state_sync import force_state_extract
from dlt.pipeline.typing import TPipelineStep
from dlt.pipeline import Pipeline


def retry_load(
    retry_on_pipeline_steps: Sequence[TPipelineStep] = ("load",)
) -> Callable[[BaseException], bool]:
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

    def _retry_load(ex: BaseException) -> bool:
        # do not retry in normalize or extract stages
        if isinstance(ex, PipelineStepFailed) and ex.step not in retry_on_pipeline_steps:
            return False
        # do not retry on terminal exceptions
        if isinstance(ex, TerminalException) or (
            ex.__context__ is not None and isinstance(ex.__context__, TerminalException)
        ):
            return False
        return True

    return _retry_load


class _DropInfo(TypedDict):
    tables: List[str]
    resource_states: List[str]
    resource_names: List[str]
    state_paths: List[str]
    schema_name: str
    dataset_name: str
    drop_all: bool
    resource_pattern: Optional[REPattern]
    warnings: List[str]


class DropCommand:
    def __init__(
        self,
        pipeline: Pipeline,
        resources: Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]] = (),
        schema_name: Optional[str] = None,
        state_paths: TAnyJsonPath = (),
        drop_all: bool = False,
        state_only: bool = False,
    ) -> None:
        self.pipeline = pipeline
        if isinstance(resources, str):
            resources = [resources]
        if isinstance(state_paths, str):
            state_paths = [state_paths]

        if not pipeline.default_schema_name:
            raise PipelineNeverRan(pipeline.pipeline_name, pipeline.pipelines_dir)

        self.schema = pipeline.schemas[schema_name or pipeline.default_schema_name].clone()
        self.schema_tables = self.schema.tables
        self.drop_tables = not state_only
        self.drop_state = True
        self.state_paths_to_drop = compile_paths(state_paths)

        resources = set(resources)
        resource_names = []
        if drop_all:
            self.resource_pattern = compile_simple_regex(TSimpleRegex("re:.*"))  # Match everything
        elif resources:
            self.resource_pattern = compile_simple_regexes(TSimpleRegex(r) for r in resources)
        else:
            self.resource_pattern = None

        if self.resource_pattern:
            data_tables = {
                t["name"]: t for t in self.schema.data_tables()
            }  # Don't remove _dlt tables
            resource_tables = group_tables_by_resource(data_tables, pattern=self.resource_pattern)
            if self.drop_tables:
                self.tables_to_drop = list(chain.from_iterable(resource_tables.values()))
                self.tables_to_drop.reverse()
            else:
                self.tables_to_drop = []
            resource_names = list(resource_tables.keys())
        else:
            self.tables_to_drop = []
            self.drop_tables = False  # No tables to drop
            self.drop_state = not not self.state_paths_to_drop  # obtain truth value

        self.drop_all = drop_all
        self.info: _DropInfo = dict(
            tables=[t["name"] for t in self.tables_to_drop],
            resource_states=[],
            state_paths=[],
            resource_names=resource_names,
            schema_name=self.schema.name,
            dataset_name=self.pipeline.dataset_name,
            drop_all=drop_all,
            resource_pattern=self.resource_pattern,
            warnings=[],
        )
        if self.resource_pattern and not resource_tables:
            self.info["warnings"].append(
                f"Specified resource(s) {str(resources)} did not select any table(s) in schema"
                f" {self.schema.name}. Possible resources are:"
                f" {list(group_tables_by_resource(data_tables).keys())}"
            )
        self._new_state = self._create_modified_state()

    @property
    def is_empty(self) -> bool:
        return (
            len(self.info["tables"]) == 0
            and len(self.info["state_paths"]) == 0
            and len(self.info["resource_states"]) == 0
        )

    def _drop_destination_tables(self) -> None:
        table_names = [tbl["name"] for tbl in self.tables_to_drop]
        for table_name in table_names:
            assert table_name not in self.schema._schema_tables, (
                f"You are dropping table {table_name} in {self.schema.name} but it is still present"
                " in the schema"
            )
        with self.pipeline._sql_job_client(self.schema) as client:
            client.drop_tables(*table_names, replace_schema=True)
            # also delete staging but ignore if staging does not exist
            if isinstance(client, WithStagingDataset):
                with contextlib.suppress(DatabaseUndefinedRelation):
                    with client.with_staging_dataset():
                        client.drop_tables(*table_names, replace_schema=True)

    def _delete_schema_tables(self) -> None:
        for tbl in self.tables_to_drop:
            del self.schema_tables[tbl["name"]]
        # bump schema, we'll save later
        self.schema._bump_version()

    def _list_state_paths(self, source_state: Dict[str, Any]) -> List[str]:
        return resolve_paths(self.state_paths_to_drop, source_state)

    def _create_modified_state(self) -> Dict[str, Any]:
        state = self.pipeline.state
        if not self.drop_state:
            return state  # type: ignore[return-value]
        source_states = _sources_state(state).items()
        for source_name, source_state in source_states:
            # drop table states
            if self.drop_state and self.resource_pattern:
                for key in _get_matching_resources(self.resource_pattern, source_state):
                    self.info["resource_states"].append(key)
                    reset_resource_state(key, source_state)
            # drop additional state paths
            resolved_paths = resolve_paths(self.state_paths_to_drop, source_state)
            if self.state_paths_to_drop and not resolved_paths:
                self.info["warnings"].append(
                    f"State paths {self.state_paths_to_drop} did not select any paths in source"
                    f" {source_name}"
                )
            _delete_source_state_keys(resolved_paths, source_state)
            self.info["state_paths"].extend(f"{source_name}.{p}" for p in resolved_paths)
        return state  # type: ignore[return-value]

    def _extract_state(self) -> None:
        state: Dict[str, Any]
        with self.pipeline.managed_state(extract_state=True) as state:  # type: ignore[assignment]
            state.clear()
            state.update(self._new_state)

    def __call__(self) -> None:
        if (
            self.pipeline.has_pending_data
        ):  # Raise when there are pending extracted/load files to prevent conflicts
            raise PipelineHasPendingDataException(
                self.pipeline.pipeline_name, self.pipeline.pipelines_dir
            )
        self.pipeline.sync_destination()

        if not self.drop_state and not self.drop_tables:
            return  # Nothing to drop

        if self.drop_tables:
            self._delete_schema_tables()
            self._drop_destination_tables()
        if self.drop_tables:
            self.pipeline.schemas.save_schema(self.schema)
        if self.drop_state:
            self._extract_state()
        # Send updated state to destination
        self.pipeline.normalize()
        try:
            self.pipeline.load(raise_on_failed_jobs=True)
        except Exception:
            # Clear extracted state on failure so command can run again
            self.pipeline.drop_pending_packages()
            with self.pipeline.managed_state() as state:
                force_state_extract(state)
            raise


def drop(
    pipeline: Pipeline,
    resources: Union[Iterable[str], str] = (),
    schema_name: str = None,
    state_paths: TAnyJsonPath = (),
    drop_all: bool = False,
    state_only: bool = False,
) -> None:
    return DropCommand(pipeline, resources, schema_name, state_paths, drop_all, state_only)()
