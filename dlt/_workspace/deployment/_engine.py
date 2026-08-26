"""Forward compatibility for the versioned manifest wire formats.

A reader cannot migrate a document produced by an engine newer than itself — the migration
entry for that version does not exist in code already deployed. It can only tolerate it,
which stays safe as long as every engine bump is additive: fields the older reader requires
are still present, and the fields it does not know about are the only new ones.
"""

import functools
import sys
from typing import Any, Set

from dlt.common import logger
from dlt.common.typing import (
    _TypedDict,
    get_args,
    get_type_hints,
    is_typeddict,
)
from dlt.common.validation import TFilterFunc


def accept_newer_engine(name: str, from_engine: int, to_engine: int) -> bool:
    """Whether a document at `from_engine` may be read as-is by code at `to_engine`."""
    if from_engine <= to_engine:
        return False
    logger.warning(
        f"{name} was written with engine version {from_engine} but this dlt version understands"
        f" {to_engine}. Reading it as-is and ignoring any fields added after engine"
        f" {to_engine}. Upgrade dlt to use them."
    )
    return True


@functools.lru_cache(maxsize=None)
def known_fields_filter(spec: Any) -> TFilterFunc:
    """`validate_dict` filter keeping only keys declared anywhere in the `spec` TypedDict tree."""
    known = _collect_keys(spec)
    return lambda k: k in known


def _collect_keys(spec: Any) -> Set[str]:
    keys: Set[str] = set()
    seen: Set[Any] = set()

    def visit(t: Any) -> None:
        if is_typeddict(t):
            if t in seen:
                return
            seen.add(t)
            hints = _type_hints(t)
            keys.update(hints.keys())
            for hint in hints.values():
                visit(hint)
            return
        # descend into List[...], Dict[str, ...], Optional[...], Union[...]
        for arg in get_args(t):
            visit(arg)

    visit(spec)
    return keys


def _type_hints(spec: _TypedDict) -> Any:
    module = sys.modules.get(spec.__module__)
    return get_type_hints(spec, globalns=module.__dict__ if module else None)
