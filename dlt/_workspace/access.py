"""Workspace access: the verbs an agent declares and a tool requires."""

import dataclasses
import typing
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from dlt.common.typing import AnyFun, NotRequired, get_args

from dlt._workspace.typing import (
    TWorkspaceAccess,
    TWorkspaceContextVerb,
    TWorkspaceDataVerb,
    TWorkspaceLocalVerb,
)

ACCESS_AXIS_VERBS: Dict[str, Tuple[str, ...]] = {
    "local": ("read", "write", "execute", "network"),
    "data": ("read", "write"),
    "context": ("read",),
}
"""Verbs a declaration may carry on each axis. `all` stands for every verb listed here."""

ACCESS_AXIS_VOCABULARY: Dict[str, Tuple[str, ...]] = {
    "local": get_args(TWorkspaceLocalVerb),
    "data": get_args(TWorkspaceDataVerb),
    "context": get_args(TWorkspaceContextVerb),
}
"""Every verb each axis carries in the type, implemented or not."""

ACCESS_AXES = tuple(ACCESS_AXIS_VERBS)

FULL_ACCESS: TWorkspaceAccess = typing.cast(
    TWorkspaceAccess,
    {"toolkits": True, **{axis: list(verbs) for axis, verbs in ACCESS_AXIS_VERBS.items()}},
)
"""Everything there is to grant."""


@dataclasses.dataclass(frozen=True)
class RequiresAccess:
    """Access a tool needs, written as `Annotated[T, RequiresAccess(data=["read"])]` on its return."""

    local: Sequence[TWorkspaceLocalVerb] = ()
    data: Sequence[TWorkspaceDataVerb] = ()
    context: Sequence[TWorkspaceContextVerb] = ()

    def __post_init__(self) -> None:
        # verbs are held as tuples: an `Annotated` alias carrying a list is unhashable
        for axis in ACCESS_AXES:
            object.__setattr__(self, axis, tuple(getattr(self, axis)))

    def as_access(self) -> TWorkspaceAccess:
        declared = {axis: list(getattr(self, axis)) for axis in ACCESS_AXES}
        return typing.cast(
            TWorkspaceAccess, {axis: verbs for axis, verbs in declared.items() if verbs}
        )


def annotation_metadata(annotation: Any) -> Tuple[Any, ...]:
    """What an `Annotated` hint carries, read through `NotRequired`."""
    if typing.get_origin(annotation) is NotRequired:
        return annotation_metadata(typing.get_args(annotation)[0])
    # only `Annotated` carries metadata; `Literal` args are values, not annotations
    return typing.cast(Tuple[Any, ...], getattr(annotation, "__metadata__", ()))


def required_access(f: AnyFun) -> TWorkspaceAccess:
    """Access the function's return annotation asks for.

    A function that declares nothing needs everything; `RequiresAccess()` asks for nothing.
    """
    try:
        hints = typing.get_type_hints(f, include_extras=True)
    except Exception:
        return FULL_ACCESS
    for metadata in annotation_metadata(hints.get("return")):
        if isinstance(metadata, RequiresAccess):
            return metadata.as_access()
    return FULL_ACCESS


def granted_verbs(access: Optional[TWorkspaceAccess], axis: str) -> Set[str]:
    """Verbs granted on one axis, with `all` expanded."""
    declared: Dict[str, Any] = dict(access or {})
    verbs = set(declared.get(axis) or [])
    return set(ACCESS_AXIS_VERBS[axis]) if "all" in verbs else verbs


def format_access(access: TWorkspaceAccess) -> str:
    """`{"data": ["read"], "toolkits": True}` as `data:read,toolkits`."""
    declared: Dict[str, Any] = dict(access or {})
    pairs = [f"{axis}:{verb}" for axis in ACCESS_AXES for verb in declared.get(axis) or []]
    return ",".join(pairs + (["toolkits"] if declared.get("toolkits") else []))


def parse_access(text: Optional[str]) -> Optional[TWorkspaceAccess]:
    """`data:read,local:write` back into an access declaration. `None` text grants everything."""
    if text is None:
        return None
    access: Dict[str, Any] = {}
    for token in text.split(","):
        token = token.strip()
        if not token:
            continue
        if token == "toolkits":
            access["toolkits"] = True
            continue
        axis, _, verb = token.partition(":")
        if axis not in ACCESS_AXES or not verb:
            raise ValueError(
                f"{token!r} is not an access value. Write axis:verb, with axis one of"
                f" {', '.join(ACCESS_AXES)}, or `toolkits`"
            )
        access.setdefault(axis, []).append(verb)
    return typing.cast(TWorkspaceAccess, access)


def missing_access(required: TWorkspaceAccess, granted: TWorkspaceAccess) -> List[str]:
    """`toolkits` and `axis:verb` grants `required` asks for and `granted` does not have."""
    missing: List[str] = (
        ["toolkits"]
        if (required or {}).get("toolkits") and not (granted or {}).get("toolkits")
        else []
    )
    for axis in ACCESS_AXES:
        verbs = granted_verbs(required, axis) - granted_verbs(granted, axis)
        missing += [f"{axis}:{verb}" for verb in sorted(verbs)]
    return missing


__all__ = [
    "ACCESS_AXES",
    "format_access",
    "parse_access",
    "ACCESS_AXIS_VERBS",
    "ACCESS_AXIS_VOCABULARY",
    "RequiresAccess",
    "annotation_metadata",
    "granted_verbs",
    "missing_access",
    "required_access",
]
