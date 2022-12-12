import base64
import binascii
import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, cast
import zlib

import pendulum

import dlt

from dlt.common import json
from dlt.common.configuration import configspec
from dlt.common.configuration.container import Container
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.specs.config_namespace_context import ConfigNamespacesContext
from dlt.common.pipeline import PipelineContext, TPipelineState
from dlt.common.typing import DictStrAny
from dlt.common.schema.typing import LOADS_TABLE_NAME, TTableSchemaColumns

from dlt.destinations.sql_client import SqlClientBase
from dlt.extract.source import DltResource

from dlt.pipeline.exceptions import PipelineStateEngineNoUpgradePathException, PipelineStateNotAvailable


# allows to upgrade state when restored with a new version of state logic/schema
STATE_ENGINE_VERSION = 2
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


class TSourceState(TPipelineState):
    sources: Dict[str, Dict[str, Any]]


@configspec(init=True)
class StateInjectableContext(ContainerInjectableContext):
    state: TPipelineState

    can_create_default: ClassVar[bool] = False

    if TYPE_CHECKING:
        def __init__(self, state: TPipelineState = None) -> None:
            ...


def merge_state_if_changed(old_state: TPipelineState, new_state: TPipelineState, increase_version: bool = True) -> Optional[TPipelineState]:
    # we may want to compare hashes like we do with schemas
    if json.dumps(old_state, sort_keys=True) == json.dumps(new_state, sort_keys=True):
        return None
    # TODO: we should probably update smarter ie. recursively
    old_state.update(new_state)
    if increase_version:
        old_state["_state_version"] += 1
    return old_state


def state_resource(state: TPipelineState) -> DltResource:
    state_str = json.dumps(state)
    state_doc = {
        "version": state["_state_version"],
        "engine_version": state["_state_engine_version"],
        "pipeline_name": state["pipeline_name"],
        "state": base64.b64encode(zlib.compress(state_str.encode("utf-8"), level=9)).decode("ascii"),
        "created_at": pendulum.now()
    }

    return dlt.resource([state_doc], name=STATE_TABLE_NAME, write_disposition="append", columns=STATE_TABLE_COLUMNS)


def load_state_from_destination(pipeline_name: str, sql_client: SqlClientBase[Any]) -> TPipelineState:
    # NOTE: if dataset or table holding state does not exist, the sql_client will rise DatabaseUndefinedRelation. caller must handle this
    query = f"SELECT state FROM {STATE_TABLE_NAME} AS s JOIN {LOADS_TABLE_NAME} AS l ON l.load_id = s._dlt_load_id WHERE pipeline_name = %s AND l.status = 0 ORDER BY created_at DESC"
    with sql_client.execute_query(query, pipeline_name) as cur:
        row = cur.fetchone()
    if not row:
        return None
    state_str = row[0]
    try:
        state_bytes = base64.b64decode(state_str, validate=True)
        state_str = zlib.decompress(state_bytes).decode("utf-8")
    except binascii.Error:
        pass
    s = json.loads(state_str)
    return migrate_state(pipeline_name, s, s["_state_engine_version"], STATE_ENGINE_VERSION)


def migrate_state(pipeline_name: str, state: DictStrAny, from_engine: int, to_engine: int) -> TPipelineState:
    if from_engine == to_engine:
        return cast(TPipelineState, state)
    if from_engine == 1 and to_engine > 1:
        state["_local"] = {}
        from_engine = 2

    # check state engine
    state["_state_engine_version"] = from_engine
    if from_engine != to_engine:
        raise PipelineStateEngineNoUpgradePathException(pipeline_name, state["_state_engine_version"], from_engine, to_engine)

    return cast(TPipelineState, state)


def state() -> DictStrAny:
    """Returns a dictionary with the source/resource state. Such state is preserved across pipeline runs and may be used to implement incremental loads.

    ### Summary
    The state is a python dictionary-like object that is available within the `@dlt.source` and `@dlt.resource` decorated functions and may be read and written to.
    The data within the state is loaded into destination together with any other extracted data and made automatically available to the source/resource extractor functions when they are run next time.
    When using the state:
    * Any JSON-serializable values can be written and the read from the state.
    * The state available in the `dlt source` is read only and any changes will be discarded. Still it may be used to initialize the resources.
    * The state available in the `dlt resource` is writable and written values will be available only once

    ### Example
    The most typical use case for the state is to implement incremental load.
    >>> @dlt.resource(write_disposition="append")
    >>> def players_games(chess_url, players, start_month=None, end_month=None):
    >>>     checked_archives = dlt.state().setdefault("archives", [])
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
    """
    global _last_full_state

    container = Container()
    # get the source name from the namespace context
    source_name: str = None
    with contextlib.suppress(ContextDefaultCannotBeCreated):
        namespaces = container[ConfigNamespacesContext].namespaces
        if namespaces and len(namespaces) > 1 and namespaces[0] == "sources":
            source_name = namespaces[1]
    try:
        # get managed state that is read/write
        state: TSourceState = container[StateInjectableContext].state  # type: ignore
    except ContextDefaultCannotBeCreated:
        # check if there's pipeline context
        proxy = container[PipelineContext]
        if not proxy.is_active():
            raise PipelineStateNotAvailable(source_name)
        else:
            # get unmanaged state that is read only
            # TODO: make sure that state if up to date by syncing the pipeline earlier
            state = proxy.pipeline().state  # type: ignore

    source_state = state.setdefault("sources", {})
    if source_name:
        source_state = source_state.setdefault(source_name, {})

    # allow inspection of last returned full state
    _last_full_state = state
    return source_state

_last_full_state: TPipelineState = None
