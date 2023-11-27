import binascii
from typing import Any, Optional, Tuple, cast
import binascii

import pendulum

import dlt

from dlt.common import json
from dlt.common.pipeline import TPipelineLocalState, TPipelineState
from dlt.common.typing import DictStrAny
from dlt.common.schema.typing import STATE_TABLE_NAME, TTableSchemaColumns
from dlt.common.destination.reference import JobClientBase, WithStateSync

from dlt.extract import DltResource

from dlt.pipeline.exceptions import PipelineStateEngineNoUpgradePathException
from dlt.common.utils import compressed_b64decode, compressed_b64encode


# allows to upgrade state when restored with a new version of state logic/schema
STATE_ENGINE_VERSION = 2

# state table columns
STATE_TABLE_COLUMNS: TTableSchemaColumns = {
    "version": {"name": "version", "data_type": "bigint", "nullable": False},
    "engine_version": {"name": "engine_version", "data_type": "bigint", "nullable": False},
    "pipeline_name": {"name": "pipeline_name", "data_type": "text", "nullable": False},
    "state": {"name": "state", "data_type": "text", "nullable": False},
    "created_at": {"name": "created_at", "data_type": "timestamp", "nullable": False},
}


def json_encode_state(state: TPipelineState) -> str:
    return json.typed_dumps(state)


def json_decode_state(state_str: str) -> DictStrAny:
    return json.typed_loads(state_str)  # type: ignore[no-any-return]


def compress_state(state: TPipelineState) -> str:
    return compressed_b64encode(json.typed_dumpb(state))


def decompress_state(state_str: str) -> DictStrAny:
    try:
        state_bytes = compressed_b64decode(state_str)
    except binascii.Error:
        return json.typed_loads(state_str)  # type: ignore[no-any-return]
    else:
        return json.typed_loadb(state_bytes)  # type: ignore[no-any-return]


def create_state_save(backup_state: TPipelineState, state: TPipelineState) -> Tuple[bool, TPipelineState, TPipelineLocalState]:
    # do not compare local states
    local_state = state.pop("_local")
    backup_state.pop("_local")

    # check if any state element was changed
    merged_state = merge_state_if_changed(backup_state, state)
    # extract state only when there's change in the state or state was not yet extracted AND we actually want to do it
    should_extract_state = merged_state is not None or "_last_extracted_at" not in local_state
    if should_extract_state:
        # print(f'EXTRACT STATE merged: {bool(merged_state)} extracted timestamp NOT in {"_last_extracted_at" not in local_state}')
        # mark state as extracted
        local_state["_last_extracted_at"] = pendulum.now()


    merged_state = merged_state or state
    return should_extract_state, merged_state, local_state


def merge_state_if_changed(
    old_state: TPipelineState, new_state: TPipelineState, increase_version: bool = True
) -> Optional[TPipelineState]:
    """Merge `new_state` into `old_state` if they differ. Otherwise returns None"""
    if json.dumpb(old_state, sort_keys=True) == json.dumpb(new_state, sort_keys=True):
        return None
    # TODO: we should probably update smarter ie. recursively
    old_state.update(new_state)
    if increase_version:
        old_state["_state_version"] += 1
    return old_state


def state_resource(state: TPipelineState) -> DltResource:
    state_str = compress_state(state)
    state_doc = {
        "version": state["_state_version"],
        "engine_version": state["_state_engine_version"],
        "pipeline_name": state["pipeline_name"],
        "state": state_str,
        "created_at": pendulum.now(),
    }
    return dlt.resource(
        [state_doc], name=STATE_TABLE_NAME, write_disposition="append", columns=STATE_TABLE_COLUMNS
    )


def load_state_from_destination(pipeline_name: str, client: WithStateSync) -> TPipelineState:
    # NOTE: if dataset or table holding state does not exist, the sql_client will rise DestinationUndefinedEntity. caller must handle this
    state = client.get_stored_state(pipeline_name)
    if not state:
        return None
    s = decompress_state(state.state)
    return migrate_state(pipeline_name, s, s["_state_engine_version"], STATE_ENGINE_VERSION)


def migrate_state(
    pipeline_name: str, state: DictStrAny, from_engine: int, to_engine: int
) -> TPipelineState:
    if from_engine == to_engine:
        return cast(TPipelineState, state)
    if from_engine == 1 and to_engine > 1:
        state["_local"] = {}
        from_engine = 2

    # check state engine
    state["_state_engine_version"] = from_engine
    if from_engine != to_engine:
        raise PipelineStateEngineNoUpgradePathException(
            pipeline_name, state["_state_engine_version"], from_engine, to_engine
        )

    return cast(TPipelineState, state)
