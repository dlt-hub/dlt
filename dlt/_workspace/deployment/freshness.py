"""Freshness constraint parsing, creators, and normalization."""

from typing import Callable, Dict, List, Mapping, NamedTuple, Sequence, Set, Union

from dlt._workspace.deployment._job_ref import resolve_job_ref
from dlt._workspace.deployment.exceptions import InvalidFreshnessConstraint
from dlt._workspace.deployment.typing import TFreshnessConstraint, TJobDefinition

__all__ = [
    "TFreshnessConstraintSpec",
    "parse_freshness_constraint",
    "is_matching_interval_fresh",
    "is_fresh",
    "normalize_freshness_constraint",
    "normalize_freshness_constraints",
    "get_direct_freshness_downstream",
    "get_transitive_freshness_downstream",
    "get_refresh_cascade_targets",
]


class TFreshnessConstraintSpec(NamedTuple):
    type: str  # noqa: A003
    expr: str


def _parse_job_is_matching_interval_fresh(expr: str) -> TFreshnessConstraintSpec:
    if not expr:
        raise InvalidFreshnessConstraint("job.is_matching_interval_fresh:", "requires a job_ref")
    if not expr.startswith("jobs."):
        raise InvalidFreshnessConstraint(
            f"job.is_matching_interval_fresh:{expr}", "expression must start with 'jobs.'"
        )
    return TFreshnessConstraintSpec(type="job.is_matching_interval_fresh", expr=expr)


def _parse_job_is_fresh(expr: str) -> TFreshnessConstraintSpec:
    if not expr:
        raise InvalidFreshnessConstraint("job.is_fresh:", "requires a job_ref")
    if not expr.startswith("jobs."):
        raise InvalidFreshnessConstraint(
            f"job.is_fresh:{expr}", "expression must start with 'jobs.'"
        )
    return TFreshnessConstraintSpec(type="job.is_fresh", expr=expr)


_FRESHNESS_PARSERS: Dict[str, Callable[[str], TFreshnessConstraintSpec]] = {
    "job.is_matching_interval_fresh": _parse_job_is_matching_interval_fresh,
    "job.is_fresh": _parse_job_is_fresh,
}


def parse_freshness_constraint(constraint: str) -> TFreshnessConstraintSpec:
    """Parse a freshness constraint string. Returns type and upstream job_ref."""
    if ":" not in constraint:
        raise InvalidFreshnessConstraint(constraint, "must be in type:expr form")
    constraint_type, expr = constraint.split(":", 1)
    parser = _FRESHNESS_PARSERS.get(constraint_type)
    if parser is None:
        raise InvalidFreshnessConstraint(constraint, f"unknown type {constraint_type!r}")
    return parser(expr)


def is_matching_interval_fresh(job_ref: str) -> TFreshnessConstraint:
    """Downstream interval must be fully covered by upstream completed intervals.

    Args:
        job_ref: Upstream job reference.
    """
    ref = resolve_job_ref(job_ref)
    _parse_job_is_matching_interval_fresh(ref)
    return TFreshnessConstraint(f"job.is_matching_interval_fresh:{ref}")


def is_fresh(job_ref: str) -> TFreshnessConstraint:
    """Upstream overall interval (intersected with downstream's) must be complete.

    Args:
        job_ref: Upstream job reference.
    """
    ref = resolve_job_ref(job_ref)
    _parse_job_is_fresh(ref)
    return TFreshnessConstraint(f"job.is_fresh:{ref}")


def normalize_freshness_constraint(
    constraint: Union[str, TFreshnessConstraint],
) -> TFreshnessConstraint:
    """Validate and normalize a single freshness constraint to canonical form."""
    s = str(constraint).strip()
    parsed = parse_freshness_constraint(s)
    return TFreshnessConstraint(f"{parsed.type}:{parsed.expr}")


def normalize_freshness_constraints(
    constraints: Union[
        None,
        str,
        TFreshnessConstraint,
        Sequence[Union[str, TFreshnessConstraint]],
    ],
) -> List[TFreshnessConstraint]:
    """Normalize freshness constraint input to a list of canonical values."""
    if constraints is None:
        return []
    if isinstance(constraints, str):
        constraints = [constraints]
    return [normalize_freshness_constraint(c) for c in constraints]


def get_direct_freshness_downstream(
    upstream_ref: str,
    all_jobs: Mapping[str, TJobDefinition],
) -> List[str]:
    """All `job_ref`s whose `freshness` list mentions `upstream_ref`.

    Args:
        upstream_ref: The upstream job reference to look up.
        all_jobs: Map of `job_ref` to job definition.

    Returns:
        List[str]: Direct downstream job refs in `all_jobs` iteration order.
        `upstream_ref` itself is never included.
    """
    out: List[str] = []
    for ref, job_def in all_jobs.items():
        if ref == upstream_ref:
            continue
        for constraint in job_def.get("freshness", []):
            try:
                fc = parse_freshness_constraint(constraint)
            except InvalidFreshnessConstraint:
                continue
            if fc.expr == upstream_ref:
                out.append(ref)
                break
    return out


def get_transitive_freshness_downstream(
    upstream_ref: str,
    all_jobs: Mapping[str, TJobDefinition],
) -> List[str]:
    """Breadth-first search through `freshness` edges from `upstream_ref`.

    Args:
        upstream_ref: The upstream job reference to start the walk from.
        all_jobs: Map of `job_ref` to job definition.

    Returns:
        List[str]: All transitively reachable downstream job refs, in
        breadth-first search order. `upstream_ref` itself is excluded
        even if reachable via a cycle.
    """
    seen: Set[str] = {upstream_ref}
    result: List[str] = []
    queue: List[str] = [upstream_ref]
    while queue:
        cur = queue.pop(0)
        for ds in get_direct_freshness_downstream(cur, all_jobs):
            if ds not in seen:
                seen.add(ds)
                result.append(ds)
                queue.append(ds)
    return result


def get_refresh_cascade_targets(
    root_ref: str,
    all_jobs: Mapping[str, TJobDefinition],
) -> List[str]:
    """Jobs to which a refresh signal initiated at `root_ref` propagates.

    Walks the freshness graph downstream from `root_ref` and returns the
    affected job refs. The root itself is excluded — callers handle it
    separately.

    Args:
        root_ref: The job ref initiating the cascade.
        all_jobs: Map of `job_ref` to `TJobDefinition`.

    Returns:
        List[str]: Breadth-first search ordered downstream refs, excluding
        the root, `refresh="block"` nodes, and interval-store-eligible jobs.
    """
    seen: Set[str] = {root_ref}
    result: List[str] = []
    queue: List[str] = [root_ref]
    while queue:
        cur = queue.pop(0)
        for ds_ref in get_direct_freshness_downstream(cur, all_jobs):
            if ds_ref in seen:
                continue
            ds_def = all_jobs.get(ds_ref)
            if ds_def is None:
                continue
            # `refresh="block"` severs the freshness chain: the block node is
            # not refreshed and its downstream is not recursed into
            if ds_def.get("refresh") == "block":
                continue
            # interval-store-eligible jobs manage their own watermark and
            # are not part of the refresh cascade
            if "interval" in ds_def and ds_def.get("allow_external_schedulers", False):
                continue
            seen.add(ds_ref)
            result.append(ds_ref)
            queue.append(ds_ref)
    return result
