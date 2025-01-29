import base64
import hashlib
import binascii
from copy import copy
from typing import List, Tuple, Mapping

from dlt.common.json import json
from dlt.common.typing import DictStrAny
from dlt.common.utils import compressed_b64decode, compressed_b64encode
from dlt.common.typing import TypedDict


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
    content = json.typed_dumpb(state_copy, sort_keys=True)
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


def json_encode_state(state: TVersionedState) -> str:
    return json.typed_dumps(state)


def json_decode_state(state_str: str) -> DictStrAny:
    return json.typed_loads(state_str)  # type: ignore[no-any-return]


def compress_state(state: TVersionedState) -> str:
    return compressed_b64encode(json.typed_dumpb(state))


def decompress_state(state_str: str) -> DictStrAny:
    try:
        state_bytes = compressed_b64decode(state_str)
    except binascii.Error:
        return json.typed_loads(state_str)  # type: ignore[no-any-return]
    else:
        return json.typed_loadb(state_bytes)  # type: ignore[no-any-return]
