import binascii
from copy import copy
from typing import Tuple, cast, List
import pendulum

import dlt
from dlt.common import json
from dlt.common.typing import DictStrAny
from dlt.common.schema.typing import STATE_TABLE_NAME, TTableSchemaColumns
from dlt.common.destination.reference import WithStateSync, Destination
from dlt.common.utils import compressed_b64decode, compressed_b64encode
from dlt.common.versioned_state import (
    generate_state_version_hash,
    bump_state_version_if_modified,
    default_versioned_state,
)
from dlt.common.pipeline import TPipelineState

from dlt.extract import DltResource

from dlt.pipeline.exceptions import (
    PipelineStateEngineNoUpgradePathException,
)

PIPELINE_STATE_ENGINE_VERSION = 4

# state table columns
STATE_TABLE_COLUMNS: TTableSchemaColumns = {
    "version": {"name": "version", "data_type": "bigint", "nullable": False},
    "engine_version": {"name": "engine_version", "data_type": "bigint", "nullable": False},
    "pipeline_name": {"name": "pipeline_name", "data_type": "text", "nullable": False},
    "state": {"name": "state", "data_type": "text", "nullable": False},
    "created_at": {"name": "created_at", "data_type": "timestamp", "nullable": False},
    "version_hash": {
        "name": "version_hash",
        "data_type": "text",
        "nullable": True,
    },  # set to nullable so we can migrate existing tables
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


def generate_pipeline_state_version_hash(state: TPipelineState) -> str:
    return generate_state_version_hash(state, exclude_attrs=["_local"])


def bump_pipeline_state_version_if_modified(state: TPipelineState) -> Tuple[int, str, str]:
    return bump_state_version_if_modified(state, exclude_attrs=["_local"])


def migrate_pipeline_state(
    pipeline_name: str, state: DictStrAny, from_engine: int, to_engine: int
) -> TPipelineState:
    if from_engine == to_engine:
        return cast(TPipelineState, state)
    if from_engine == 1 and to_engine > 1:
        state["_local"] = {}
        from_engine = 2
    if from_engine == 2 and to_engine > 2:
        # you may want to recompute hash
        state["_version_hash"] = generate_pipeline_state_version_hash(state)  # type: ignore[arg-type]
        from_engine = 3
    if from_engine == 3 and to_engine > 3:
        if state.get("destination"):
            state["destination_type"] = state["destination"]
            state["destination_name"] = Destination.to_name(state["destination"])
            del state["destination"]
        if state.get("staging"):
            state["staging_type"] = state["staging"]
            state["staging_name"] = Destination.to_name(state["staging"])
            del state["staging"]
        from_engine = 4

    # check state engine
    if from_engine != to_engine:
        raise PipelineStateEngineNoUpgradePathException(
            pipeline_name, state["_state_engine_version"], from_engine, to_engine
        )
    state["_state_engine_version"] = from_engine
    return cast(TPipelineState, state)


def state_resource(state: TPipelineState) -> DltResource:
    state = copy(state)
    state.pop("_local")
    state_str = compress_state(state)
    state_doc = {
        "version": state["_state_version"],
        "engine_version": state["_state_engine_version"],
        "pipeline_name": state["pipeline_name"],
        "state": state_str,
        "created_at": pendulum.now(),
        "version_hash": state["_version_hash"],
    }
    return dlt.resource(
        [state_doc], name=STATE_TABLE_NAME, write_disposition="append", columns=STATE_TABLE_COLUMNS
    )


def load_pipeline_state_from_destination(
    pipeline_name: str, client: WithStateSync
) -> TPipelineState:
    # NOTE: if dataset or table holding state does not exist, the sql_client will rise DestinationUndefinedEntity. caller must handle this
    state = client.get_stored_state(pipeline_name)
    if not state:
        return None
    s = decompress_state(state.state)
    return migrate_pipeline_state(
        pipeline_name, s, s["_state_engine_version"], PIPELINE_STATE_ENGINE_VERSION
    )


def default_pipeline_state() -> TPipelineState:
    return {
        **default_versioned_state(),
        "_state_engine_version": PIPELINE_STATE_ENGINE_VERSION,
        "_local": {"first_run": True},
    }
