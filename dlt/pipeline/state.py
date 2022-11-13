from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, TypedDict, Optional, cast

import dlt

from dlt.common import json, pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs.base_configuration import configspec
from dlt.common.pipeline import PipelineContext
from dlt.common.typing import DictStrAny
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import SqlClientBase

from dlt.extract.source import DltResource

from dlt.pipeline.exceptions import PipelineStateEngineNoUpgradePathException, PipelineStateNotAvailable


# allows to upgrade state when restored with a new version of state logic/schema
STATE_ENGINE_VERSION = 1
# state table name
STATE_TABLE_NAME = "_dlt_pipeline_state"
# state table columns
STATE_TABLE_COLUMNS: TTableSchemaColumns = {
    "version": {
        "name": "version",
        "data_type": "bigint",
        "nullable": False
    },
    "engine_version": {
        "name": "engine_version",
        "data_type": "bigint",
        "nullable": False
    },
    "pipeline_name": {
        "name": "pipeline_name",
        "data_type": "text",
        "nullable": False
    },
    "state": {
        "name": "state",
        "data_type": "text",
        "nullable": False
    },
    "created_at": {
        "name": "created_at",
        "data_type": "timestamp",
        "nullable": False
    }
}

class TPipelineState(TypedDict, total=False):
    pipeline_name: str
    dataset_name: str
    default_schema_name: Optional[str]
    schema_names: Optional[List[str]]
    destination: Optional[str]
    # properties starting with _ are not automatically applied to pipeline object when state is restored
    _state_version: int
    _state_engine_version: int


class TSourceState(TPipelineState):
    sources: Dict[str, Dict[str, Any]]


@configspec(init=True)
class StateInjectableContext(ContainerInjectableContext):
    state: TPipelineState

    can_create_default: ClassVar[bool] = False

    if TYPE_CHECKING:
        def __init__(self, state: TPipelineState = None) -> None:
            ...


def merge_state(old_state: TPipelineState, new_state: TPipelineState) -> TPipelineState:
    # load state from storage to be merged with pipeline changes, currently we assume no parallel changes
    # TODO: we should probably update smarter ie. recursively
    old_state.update(new_state)
    old_state["_state_version"] += 1
    return old_state


def state_resource(state: TPipelineState) -> DltResource:
    state_doc = {
        "version": state["_state_version"],
        "engine_version": state["_state_engine_version"],
        "pipeline_name": state["pipeline_name"],
        "state": json.dumps(state),
        "created_at": pendulum.now()
    }

    return dlt.resource([state_doc], name=STATE_TABLE_NAME, write_disposition="append", columns=STATE_TABLE_COLUMNS)


def load_state_from_destination(pipeline_name: str, sql_client: SqlClientBase[Any]) -> TPipelineState:
    try:
        query = f"SELECT state FROM {STATE_TABLE_NAME} WHERE pipeline_name = %s ORDER BY created_at DESC"
        with sql_client.execute_query(query, pipeline_name) as cur:
            row = cur.fetchone()
        if not row:
            return None
        state = cast(TPipelineState, json.loads(row[0]))
        # check state engine
        if state["_state_engine_version"] != STATE_ENGINE_VERSION:
            raise PipelineStateEngineNoUpgradePathException(pipeline_name, state["_state_engine_version"], state["_state_engine_version"], STATE_ENGINE_VERSION)
        return state
    except DatabaseUndefinedRelation:
        # state will not load and will not be available
        return None


def state() -> DictStrAny:
    container = Container()
    try:
        state: TSourceState = container[StateInjectableContext].state  # type: ignore
        # TODO: take source context and get dict key by source name
        return state.setdefault("sources", {})
    except ContextDefaultCannotBeCreated:
        # check if there's pipeline context
        proxy = container[PipelineContext]
        raise PipelineStateNotAvailable(proxy.is_active())
