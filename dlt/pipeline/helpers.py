from collections import defaultdict
from typing import Callable, Tuple, Iterable, Optional, Any, cast

from dlt.common.exceptions import TerminalException
from dlt.common.schema.utils import get_child_tables

from dlt.pipeline.exceptions import PipelineStepFailed
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


class DropCommand:
    def __init__(
        self,
        pipeline: Pipeline,
        # tables: Optional[Iterable[str]] = None,
        resources: Optional[Iterable[str]] = None,
        schema_name: str = None,
    ) -> None:
        self.pipeline = pipeline
        # self.tables = set(tables or [])
        resources = set(resources or [])
        self.schema = pipeline.schemas[schema_name or pipeline.default_schema_name]
        self.schema_tables = self.schema.tables

        drop_tables = []
        for tbl in self.schema_tables.values():
            if tbl.get('resource') in resources:
                drop_tables += get_child_tables(self.schema_tables, tbl['name'])
        self.drop_tables = list(reversed(drop_tables))


        # # Create a flat list of parent and child tables to drop
        # drop_tables = []
        # for t in self.tables:
        #     if t in self.schema_tables:
        #         drop_tables += [c for c in get_child_tables(self.schema_tables, t)]
        # self.drop_tables = list(reversed(drop_tables))  # Reverse so children are dropped before parent

        # # List resource state keys to drop
        # drop_resources = set(r for r in (t.get('resource') for t in self.drop_tables) if r)

        self.drop_resource_state_keys = resources

    def drop_destination_tables(self) -> None:
        with self.pipeline._get_destination_client(self.schema) as client:
            client.drop_tables([tbl['name'] for tbl in self.drop_tables])

    def delete_pipeline_tables(self) -> None:
        for tbl in self.drop_tables:
            del self.schema_tables[tbl['name']]

    def drop_state_keys(self) -> None:
        state: TSourceState
        with self.pipeline.managed_state(extract_state=True) as state:  # type: ignore[assignment]
            source_states = state.get("sources")
            if not source_states:
                return
            source_state = source_states.get(self.schema.name)
            if not source_state:
                return
            resources = source_state.get('resources')
            if not resources:
                return
            for key in self.drop_resource_state_keys:
                resources.pop(key, None)

    def info(self) -> Any:
        # TODO: Return list of tables, source keys, etc that would be dropped
        return dict(
            drop_tables=[tbl['name'] for tbl in self.drop_tables],
        )

    def __call__(self) -> None:
        self.delete_pipeline_tables()
        self.schema.bump_version()
        self.drop_destination_tables()
        self.drop_state_keys()
        # TODO: (all tables dissappear here) see _update_live_schema() -> replace_schema_content
        # live schema is same instance
        # should be working on a copy
        self.pipeline.schemas.save_schema(self.schema)
        # Send extracted state to destination
        # TODO: Other load packages will go too. Is this bad?
        self.pipeline.load()


def drop(
    pipeline: Pipeline,
    # tables: Optional[Iterable[str]] = None,
    resources: Optional[Iterable[str]] = None,
    schema_name: str = None,
) -> None:
    return DropCommand(pipeline, resources, schema_name)()
