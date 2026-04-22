"""Tests for freshness-graph walks used by interval jobs and refresh cascades.

Covers `get_direct_freshness_downstream`, `get_transitive_freshness_downstream`,
and `get_refresh_cascade_targets` — the helpers that traverse the job dependency
graph defined by `job.is_fresh` / `job.is_matching_interval_fresh` constraints.
"""

from typing import Dict, List, Optional

import pytest

from dlt._workspace.deployment.freshness import (
    get_direct_freshness_downstream,
    get_refresh_cascade_targets,
    get_transitive_freshness_downstream,
)
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TFreshnessConstraint,
    TIntervalSpec,
    TJobDefinition,
    TJobRef,
    TJobType,
    TRefreshPolicy,
    TTrigger,
)


def _job(
    ref: str,
    triggers: Optional[List[str]] = None,
    interval: Optional[TIntervalSpec] = None,
    default_trigger: Optional[str] = None,
    job_type: TJobType = "batch",
    freshness: Optional[List[str]] = None,
    refresh: Optional[TRefreshPolicy] = None,
    allow_external_schedulers: Optional[bool] = None,
) -> TJobDefinition:
    job: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(
            module="m",
            function="f",
            job_type=job_type,
            launcher="dlt._workspace.deployment.launchers.job",
        ),
        "triggers": [TTrigger(t) for t in (triggers or [])],
        "execute": TExecuteSpec(),
    }
    if interval is not None:
        job["interval"] = interval
    if default_trigger is not None:
        job["default_trigger"] = TTrigger(default_trigger)
    if freshness is not None:
        job["freshness"] = [TFreshnessConstraint(c) for c in freshness]
    if refresh is not None:
        job["refresh"] = refresh
    if allow_external_schedulers is not None:
        job["allow_external_schedulers"] = allow_external_schedulers
    return job


def _make_chain(*refs: str) -> Dict[str, TJobDefinition]:
    """Linear chain a → b → c → ... where each downstream has freshness on its predecessor."""
    jobs: Dict[str, TJobDefinition] = {}
    prev: Optional[str] = None
    for ref in refs:
        jobs[ref] = _job(
            ref,
            ["manual:" + ref],
            freshness=[f"job.is_fresh:{prev}"] if prev else None,
        )
        prev = ref
    return jobs


def _diamond_jobs() -> Dict[str, TJobDefinition]:
    """a → {b, c} → d (b and c both downstream of a, d downstream of both)."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", ["manual:jobs.c"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.d": _job(
            "jobs.d",
            ["manual:jobs.d"],
            freshness=["job.is_fresh:jobs.b", "job.is_fresh:jobs.c"],
        ),
    }


def _self_loop_jobs() -> Dict[str, TJobDefinition]:
    """Single job with freshness pointing at itself."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"], freshness=["job.is_fresh:jobs.a"]),
    }


def _invalid_constraint_jobs() -> Dict[str, TJobDefinition]:
    """jobs.b has one malformed constraint and one valid constraint on jobs.a."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job(
            "jobs.b",
            ["manual:jobs.b"],
            freshness=["not-a-constraint", "job.is_fresh:jobs.a"],
        ),
    }


def _cycle_jobs() -> Dict[str, TJobDefinition]:
    """Cycle a → b → c → a (each freshness points back one hop)."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"], freshness=["job.is_fresh:jobs.c"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job("jobs.c", ["manual:jobs.c"], freshness=["job.is_fresh:jobs.b"]),
    }


def _disconnected_jobs() -> Dict[str, TJobDefinition]:
    """Two jobs with no freshness edges between them."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"]),
    }


@pytest.mark.parametrize(
    "jobs,query_ref,expected",
    [
        # chain: a → b → c
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.a", ["jobs.b"]),
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.b", ["jobs.c"]),
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.c", []),
        # self-loop: a → a is suppressed
        (_self_loop_jobs(), "jobs.a", []),
        # diamond: a → {b, c} → d
        (_diamond_jobs(), "jobs.a", ["jobs.b", "jobs.c"]),
        (_diamond_jobs(), "jobs.b", ["jobs.d"]),
        (_diamond_jobs(), "jobs.c", ["jobs.d"]),
        (_diamond_jobs(), "jobs.d", []),
        # malformed constraints are silently ignored, valid ones still match
        (_invalid_constraint_jobs(), "jobs.a", ["jobs.b"]),
        # disconnected
        (_disconnected_jobs(), "jobs.a", []),
    ],
    ids=[
        "chain-from-a",
        "chain-from-b",
        "chain-leaf",
        "self-loop",
        "diamond-from-a",
        "diamond-from-b",
        "diamond-from-c",
        "diamond-leaf",
        "invalid-constraint-ignored",
        "disconnected",
    ],
)
def test_get_direct_freshness_downstream(
    jobs: Dict[str, TJobDefinition],
    query_ref: str,
    expected: List[str],
) -> None:
    assert get_direct_freshness_downstream(query_ref, jobs) == expected


@pytest.mark.parametrize(
    "jobs,query_ref,expected",
    [
        # chain (4 nodes): full BFS order
        (
            _make_chain("jobs.a", "jobs.b", "jobs.c", "jobs.d"),
            "jobs.a",
            ["jobs.b", "jobs.c", "jobs.d"],
        ),
        # diamond: d appears exactly once even though both b and c reach it
        (_diamond_jobs(), "jobs.a", ["jobs.b", "jobs.c", "jobs.d"]),
        # cycle a → b → c → a: seed is excluded even when reachable via the cycle
        (_cycle_jobs(), "jobs.a", ["jobs.b", "jobs.c"]),
        # disconnected
        (_disconnected_jobs(), "jobs.a", []),
        # leaf in chain has no downstream
        (_make_chain("jobs.a", "jobs.b", "jobs.c"), "jobs.c", []),
    ],
    ids=["chain", "diamond", "cycle-seed-excluded", "disconnected", "leaf"],
)
def test_get_transitive_freshness_downstream(
    jobs: Dict[str, TJobDefinition],
    query_ref: str,
    expected: List[str],
) -> None:
    assert get_transitive_freshness_downstream(query_ref, jobs) == expected


def _chain_with_block_middle() -> Dict[str, TJobDefinition]:
    """A → B(auto) → C(block) → D(auto). The walk severs at C."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job(
            "jobs.c",
            ["manual:jobs.c"],
            freshness=["job.is_fresh:jobs.b"],
            refresh="block",
        ),
        "jobs.d": _job("jobs.d", ["manual:jobs.d"], freshness=["job.is_fresh:jobs.c"]),
    }


def _diamond_with_blocked_branch() -> Dict[str, TJobDefinition]:
    """A → {B(auto), C(block)} → D(auto). D is reachable via B."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
        "jobs.c": _job(
            "jobs.c",
            ["manual:jobs.c"],
            freshness=["job.is_fresh:jobs.a"],
            refresh="block",
        ),
        "jobs.d": _job(
            "jobs.d",
            ["manual:jobs.d"],
            freshness=["job.is_fresh:jobs.b", "job.is_fresh:jobs.c"],
        ),
    }


def _chain_with_always_middle() -> Dict[str, TJobDefinition]:
    """A → B(always) → C(auto). always mid-walk is treated as auto."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job(
            "jobs.b",
            ["manual:jobs.b"],
            freshness=["job.is_fresh:jobs.a"],
            refresh="always",
        ),
        "jobs.c": _job("jobs.c", ["manual:jobs.c"], freshness=["job.is_fresh:jobs.b"]),
    }


def _chain_with_interval_middle() -> Dict[str, TJobDefinition]:
    """A → B(interval-store) → C(auto). B is excluded; the walk does not pass through it."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"]),
        "jobs.b": _job(
            "jobs.b",
            ["schedule:0 * * * *"],
            interval={"start": "2024-01-01T00:00:00Z"},
            allow_external_schedulers=True,
            freshness=["job.is_fresh:jobs.a"],
        ),
        "jobs.c": _job("jobs.c", ["manual:jobs.c"], freshness=["job.is_fresh:jobs.b"]),
    }


def _self_cycle_two_jobs() -> Dict[str, TJobDefinition]:
    """A → B → A — verifies the seen-set prevents infinite loops."""
    return {
        "jobs.a": _job("jobs.a", ["manual:jobs.a"], freshness=["job.is_fresh:jobs.b"]),
        "jobs.b": _job("jobs.b", ["manual:jobs.b"], freshness=["job.is_fresh:jobs.a"]),
    }


@pytest.mark.parametrize(
    "jobs,query_ref,expected",
    [
        # empty graph
        ({}, "jobs.a", []),
        # singleton root, no downstream
        ({"jobs.a": _job("jobs.a", ["manual:jobs.a"])}, "jobs.a", []),
        # linear all-auto chain — full BFS order
        (
            _make_chain("jobs.a", "jobs.b", "jobs.c", "jobs.d"),
            "jobs.a",
            ["jobs.b", "jobs.c", "jobs.d"],
        ),
        # block in middle of a chain — walk severed
        (_chain_with_block_middle(), "jobs.a", ["jobs.b"]),
        # diamond around a blocked branch — D still reached via B
        (_diamond_with_blocked_branch(), "jobs.a", ["jobs.b", "jobs.d"]),
        # always mid-walk is treated as auto (no amplification)
        (_chain_with_always_middle(), "jobs.a", ["jobs.b", "jobs.c"]),
        # interval-store mid-walk excludes the node and severs the walk
        (_chain_with_interval_middle(), "jobs.a", []),
        # cycle safety — A → B → A
        (_self_cycle_two_jobs(), "jobs.a", ["jobs.b"]),
        # root not in all_jobs — no error, empty list
        ({}, "jobs.nope", []),
    ],
    ids=[
        "empty-graph",
        "singleton-no-downstream",
        "linear-chain-all-auto",
        "block-in-middle",
        "diamond-around-block",
        "always-mid-walk-is-auto",
        "interval-store-mid-walk",
        "cycle-safety",
        "root-not-in-all-jobs",
    ],
)
def test_get_refresh_cascade_targets(
    jobs: Dict[str, TJobDefinition],
    query_ref: str,
    expected: List[str],
) -> None:
    assert get_refresh_cascade_targets(query_ref, jobs) == expected
