"""Freshness constraint parsing, creators, and normalization."""

from typing import Callable, Dict, List, NamedTuple, Sequence, Union

from dlt._workspace.deployment._job_ref import resolve_job_ref
from dlt._workspace.deployment.exceptions import InvalidFreshnessConstraint
from dlt._workspace.deployment.typing import TFreshnessConstraint

__all__ = [
    "TFreshnessConstraintSpec",
    "parse_freshness_constraint",
    "is_matching_interval_fresh",
    "is_fresh",
    "normalize_freshness_constraint",
    "normalize_freshness_constraints",
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
