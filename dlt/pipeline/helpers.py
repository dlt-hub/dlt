import contextlib
from typing import (
    Callable,
    Sequence,
    Iterable,
    Optional,
    Any,
    List,
    Dict,
    Union,
    TypedDict,
    TYPE_CHECKING,
)
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
    StateInjectableContext,
    Container,
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
from dlt.pipeline.drop import drop_resources
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated

if TYPE_CHECKING:
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


class DropCommand:
    def __init__(
        self,
        pipeline: "Pipeline",
        resources: Union[Iterable[Union[str, TSimpleRegex]], Union[str, TSimpleRegex]] = (),
        schema_name: Optional[str] = None,
        state_paths: TAnyJsonPath = (),
        drop_all: bool = False,
        state_only: bool = False,
    ) -> None:
        """
        Args:
            pipeline: Pipeline to drop tables and state from
            resources: List of resources to drop. If empty, no resources are dropped unless `drop_all` is True
            schema_name: Name of the schema to drop tables from. If not specified, the default schema is used
            state_paths: JSON path(s) relative to the source state to drop
            drop_all: Drop all resources and tables in the schema (supersedes `resources` list)
            state_only: Drop only state, not tables
        """
        self.pipeline = pipeline

        if not pipeline.default_schema_name:
            raise PipelineNeverRan(pipeline.pipeline_name, pipeline.pipelines_dir)
        self.schema = pipeline.schemas[schema_name or pipeline.default_schema_name]

        self.drop_tables = not state_only

        self._drop_schema, self._new_state, self.info = drop_resources(
            self.schema,
            pipeline.state,
            resources,
            state_paths,
            drop_all,
            state_only,
        )

        self.drop_state = bool(drop_all or resources or state_paths)

    @property
    def is_empty(self) -> bool:
        return (
            len(self.info["tables"]) == 0
            and len(self.info["state_paths"]) == 0
            and len(self.info["resource_states"]) == 0
        )

    def _drop_destination_tables(self, allow_schema_tables: bool = False) -> None:
        table_names = self.info["tables"]
        if not allow_schema_tables:
            for table_name in table_names:
                assert table_name not in self.schema._schema_tables, (
                    f"You are dropping table {table_name} in {self.schema.name} but it is still"
                    " present in the schema"
                )
        with self.pipeline._sql_job_client(self.schema) as client:
            client.drop_tables(*table_names, replace_schema=True)
            # also delete staging but ignore if staging does not exist
            if isinstance(client, WithStagingDataset):
                with contextlib.suppress(DatabaseUndefinedRelation):
                    with client.with_staging_dataset():
                        client.drop_tables(*table_names, replace_schema=True)

    def _delete_schema_tables(self) -> None:
        for tbl in self.info["tables"]:
            del self.schema.tables[tbl]
        # bump schema, we'll save later
        self.schema._bump_version()

    def _extract_state(self) -> None:
        state: Dict[str, Any]
        with self.pipeline.managed_state(extract_state=True) as state:  # type: ignore[assignment]
            state.clear()
            state.update(self._new_state)
        try:
            # Also update the state in current context if one is active
            # so that we can run the pipeline directly after drop in the same process
            ctx = Container()[StateInjectableContext]
            state = ctx.state  # type: ignore[assignment]
            state.clear()
            state.update(self._new_state)
        except ContextDefaultCannotBeCreated:
            pass

    def _save_local_schema(self) -> None:
        self.pipeline.schemas.save_schema(self.schema)

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
            self._save_local_schema()
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
    pipeline: "Pipeline",
    resources: Union[Iterable[str], str] = (),
    schema_name: str = None,
    state_paths: TAnyJsonPath = (),
    drop_all: bool = False,
    state_only: bool = False,
) -> None:
    return DropCommand(pipeline, resources, schema_name, state_paths, drop_all, state_only)()
