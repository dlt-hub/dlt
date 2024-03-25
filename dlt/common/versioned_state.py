import base64
import hashlib
from copy import copy

import datetime  # noqa: 251
from dlt.common import json
from typing import TypedDict, Dict, Any, List, Tuple, cast


class TVersionedState(TypedDict, total=False):
    _state_version: int
    _version_hash: str
    _state_engine_version: int


def generate_state_version_hash(state: TVersionedState, exclude_attrs: List[str] = None) -> str:
    # generates hash out of stored schema content, excluding hash itself, version and local state
    state_copy = copy(state)
    exclude_attrs = exclude_attrs or []
    exclude_attrs.extend(["_state_version", "_state_engine_version", "_version_hash"])
    for attr in exclude_attrs:
        state_copy.pop(attr, None)  # type: ignore
    content = json.typed_dumpb(state_copy, sort_keys=True)  # type: ignore
    h = hashlib.sha3_256(content)
    return base64.b64encode(h.digest()).decode("ascii")


def bump_state_version_if_modified(
    state: TVersionedState, exclude_attrs: List[str] = None
) -> Tuple[int, str, str]:
    """Bumps the `state` version and version hash if content modified, returns (new version, new hash, old hash) tuple"""
    hash_ = generate_state_version_hash(state, exclude_attrs)
    previous_hash = state.get("_version_hash")
    if not previous_hash:
        # if hash was not set, set it without bumping the version, that's the initial state
        pass
    elif hash_ != previous_hash:
        state["_state_version"] += 1

    state["_version_hash"] = hash_
    return state["_state_version"], hash_, previous_hash


def default_versioned_state() -> TVersionedState:
    return {"_state_version": 0, "_state_engine_version": 1}
