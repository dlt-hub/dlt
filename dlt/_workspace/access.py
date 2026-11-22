import dataclasses
import typing
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from dlt.common.typing import AnyFun, annotation_metadata, get_args

from dlt._workspace.typing import (
    TProfileDataAccess,
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
    TWorkspaceAccess, {axis: list(verbs) for axis, verbs in ACCESS_AXIS_VERBS.items()}
)
"""Everything there is to grant."""

DATA_ACCESS_LEVELS = typing.cast(
    Tuple[TProfileDataAccess, ...], ("none",) + ACCESS_AXIS_VERBS["data"]
)
"""Data access levels, the lowest first: `none`, then the data verbs of job access."""


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


def required_access(f: AnyFun) -> TWorkspaceAccess:
    """Access that the function's return annotation asks for."""
    try:
        hints = typing.get_type_hints(f, include_extras=True)
    except Exception:
        return FULL_ACCESS
    for metadata in annotation_metadata(hints.get("return")):
        if isinstance(metadata, RequiresAccess):
            return metadata.as_access()
    # a function that declares nothing needs everything; `RequiresAccess()` asks for nothing
    return FULL_ACCESS


def normalize_access(declared: Any, name: str = "access") -> TWorkspaceAccess:
    """Validates an access declaration and returns it with a single verb of an axis as a list.

    Args:
        declared (Any): The declaration, a mapping of axes to verbs.
        name (str): How messages name the declaration, for example `access` or `__access__`.

    Raises:
        ValueError: The declaration is not a mapping, names an unknown axis, or a verb that its
            axis does not take.
    """
    if not isinstance(declared, Mapping):
        raise ValueError(f"{name} takes a mapping of {', '.join(ACCESS_AXES)} to verbs")
    access: Dict[str, Any] = dict(declared)
    if unknown := sorted(set(access) - set(ACCESS_AXES)):
        raise ValueError(f"{name} takes {', '.join(ACCESS_AXES)}. Got {', '.join(unknown)}")
    for axis in ACCESS_AXES:
        if isinstance(access.get(axis), str):
            access[axis] = [access[axis]]
        allowed = ACCESS_AXIS_VERBS[axis] + ("all",)
        refused = sorted({verb for verb in access.get(axis) or [] if verb not in allowed})
        if refused:
            unserved = sorted(set(ACCESS_AXIS_VOCABULARY[axis]) - set(allowed))
            reason = f"{name}.{axis} takes {', '.join(allowed)}"
            if set(unserved) & set(refused):
                reason += f", and no runtime serves {', '.join(unserved)} yet"
            raise ValueError(f"{reason}. Got {', '.join(refused)}")
    return typing.cast(TWorkspaceAccess, access)


def granted_verbs(access: Optional[TWorkspaceAccess], axis: str) -> Set[str]:
    """Verbs granted on one axis, with `all` expanded."""
    declared: Dict[str, Any] = dict(access or {})
    verbs = set(declared.get(axis) or [])
    return set(ACCESS_AXIS_VERBS[axis]) if "all" in verbs else verbs


def get_data_access_level(access: Optional[TWorkspaceAccess]) -> TProfileDataAccess:
    """Data access that `access` declares: `write` when it declares `write`, else `read`, else
    `none`."""
    verbs = granted_verbs(access, "data")
    if "write" in verbs:
        return "write"
    return "read" if "read" in verbs else "none"


def get_data_access_verbs(data_access: str) -> List[TWorkspaceDataVerb]:
    """Data verbs that `data_access` includes. `write` and `all` include `read`."""
    if data_access in ("write", "all"):
        return ["read", "write"]
    return ["read"] if data_access == "read" else []


def format_data_access(data_access: str) -> str:
    return "no data access" if data_access == "none" else f"{data_access} data access"


def format_access(access: TWorkspaceAccess) -> str:
    """Formats `{"data": ["read"], "local": ["write"]}` as `data:read,local:write`."""
    declared: Dict[str, Any] = dict(access or {})
    tokens = [f"{axis}:{verb}" for axis in ACCESS_AXES for verb in declared.get(axis) or []]
    # an access granting nothing renders as `local:,data:,context:`
    return ",".join(tokens or (f"{axis}:" for axis in ACCESS_AXES))


def parse_access(text: str) -> TWorkspaceAccess:
    """Parses `data:read,local:write` into an access declaration.

    Raises:
        ValueError: The text is empty or a token is not `axis:verb`.
    """
    access: Dict[str, Any] = {}
    declared = False
    for token in (text or "").split(","):
        token = token.strip()
        if not token:
            continue
        axis, colon, verb = token.partition(":")
        if axis not in ACCESS_AXES or not colon:
            raise ValueError(
                f"{token!r} is not an access value. Write axis:verb, with axis one of"
                f" {', '.join(ACCESS_AXES)}"
            )
        declared = True
        # `axis:` alone declares the axis and grants nothing on it
        if verb:
            access.setdefault(axis, []).append(verb)
    # empty text is refused: `axis:` grants nothing, and a caller granting all passes `FULL_ACCESS`
    if not declared:
        raise ValueError(f"{text!r} declares no access. Write axis:verb pairs, or axis: for none")
    return typing.cast(TWorkspaceAccess, access)


def missing_access(required: TWorkspaceAccess, granted: TWorkspaceAccess) -> List[str]:
    """`axis:verb` grants `required` asks for and `granted` does not have."""
    missing: List[str] = []
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
    "DATA_ACCESS_LEVELS",
    "RequiresAccess",
    "get_data_access_level",
    "get_data_access_verbs",
    "format_data_access",
    "granted_verbs",
    "missing_access",
    "required_access",
]
