from copy import copy
from typing import Tuple, cast

import dlt
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.common.schema.typing import STATE_TABLE_NAME, TTableSchemaColumns
from dlt.common.destination.reference import WithStateSync, Destination, StateInfo
from dlt.common.versioned_state import (
    generate_state_version_hash,
    bump_state_version_if_modified,
    default_versioned_state,
    compress_state,
    decompress_state,
)
from dlt.common.pipeline import TPipelineState
from dlt.common.storages.load_package import TPipelineStateDoc
from dlt.extract import DltResource

from dlt.pipeline.exceptions import (
    PipelineStateEngineNoUpgradePathException,
)

PIPELINE_STATE_ENGINE_VERSION = 4
LOAD_PACKAGE_STATE_KEY = "pipeline_state"

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


def generate_pipeline_state_version_hash(state: TPipelineState) -> str:
    return generate_state_version_hash(state, exclude_attrs=["_local"])


def bump_pipeline_state_version_if_modified(state: TPipelineState) -> Tuple[int, str, str]:
    return bump_state_version_if_modified(state, exclude_attrs=["_local"])


def mark_state_extracted(state: TPipelineState, hash_: str) -> None:
    """Marks state as extracted by setting last extracted hash to hash_ (which is current version_hash)

    `_last_extracted_hash` is kept locally and never synced with the destination
    """
    state["_local"]["_last_extracted_at"] = pendulum.now()
    state["_local"]["_last_extracted_hash"] = hash_


def force_state_extract(state: TPipelineState) -> None:
    """Forces `state` to be extracted by removing local information on the most recent extraction"""
    state["_local"].pop("_last_extracted_at", None)
    state["_local"].pop("_last_extracted_hash", None)


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


def state_doc(state: TPipelineState, load_id: str = None) -> TPipelineStateDoc:
    state = copy(state)
    state.pop("_local")
    state_str = compress_state(state)
    doc: TPipelineStateDoc = {
        "version": state["_state_version"],
        "engine_version": state["_state_engine_version"],
        "pipeline_name": state["pipeline_name"],
        "state": state_str,
        "created_at": pendulum.now(),
        "version_hash": state["_version_hash"],
    }
    if load_id:
        doc["dlt_load_id"] = load_id
    return doc


def state_resource(state: TPipelineState) -> Tuple[DltResource, TPipelineStateDoc]:
    doc = state_doc(state)
    return (
        dlt.resource(
            [doc],
            name=STATE_TABLE_NAME,
            write_disposition="append",
            columns=STATE_TABLE_COLUMNS,
        ),
        doc,
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
