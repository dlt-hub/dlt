"""Tests for freshness primitives — interval-store iteration and freshness-graph walks.

Covers:
- Interval-store iteration (`iter_intervals`, `sort_and_coalesce`,
  `get_eligible_intervals`, `next_eligible_interval`) used to detect which
  cron intervals an upstream job has completed.
- Freshness-graph walks (`get_direct_freshness_downstream`,
  `get_transitive_freshness_downstream`, `get_refresh_cascade_targets`) that
  traverse the job dependency graph defined by `job.is_fresh` /
  `job.is_matching_interval_fresh` constraints.
"""

from datetime import timezone  # noqa: I251
from typing import Dict, List, Optional, Tuple

import pytest

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TTimeInterval

from dlt._workspace.deployment._interval_store_freshness import (
    get_eligible_intervals,
    iter_intervals,
    next_eligible_interval,
    sort_and_coalesce,
)
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


def _iv(start: str, end: str) -> TTimeInterval:
    return (ensure_pendulum_datetime_utc(start), ensure_pendulum_datetime_utc(end))


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


@pytest.mark.parametrize(
    "intervals,expected",
    [
        # empty
        ([], []),
        # single
        ([("2024-01-01", "2024-01-02")], [("2024-01-01", "2024-01-02")]),
        # adjacent → merged
        (
            [("2024-01-01", "2024-01-02"), ("2024-01-02", "2024-01-03")],
            [("2024-01-01", "2024-01-03")],
        ),
        # overlapping → merged
        (
            [("2024-01-01", "2024-01-03"), ("2024-01-02", "2024-01-04")],
            [("2024-01-01", "2024-01-04")],
        ),
        # gap preserved
        (
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
        ),
        # unsorted input
        (
            [("2024-01-04", "2024-01-05"), ("2024-01-01", "2024-01-02")],
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
        ),
        # three adjacent → single
        (
            [
                ("2024-01-01", "2024-01-02"),
                ("2024-01-02", "2024-01-03"),
                ("2024-01-03", "2024-01-04"),
            ],
            [("2024-01-01", "2024-01-04")],
        ),
    ],
    ids=["empty", "single", "adjacent", "overlapping", "gap", "unsorted", "three-adjacent"],
)
def test_sort_and_coalesce(
    intervals: List[Tuple[str, str]], expected: List[Tuple[str, str]]
) -> None:
    ivs = [(ensure_pendulum_datetime_utc(s), ensure_pendulum_datetime_utc(e)) for s, e in intervals]
    exp = [(ensure_pendulum_datetime_utc(s), ensure_pendulum_datetime_utc(e)) for s, e in expected]
    assert sort_and_coalesce(ivs) == exp


@pytest.mark.parametrize(
    "cron,overall,expected_count,first_start,first_end",
    [
        ("0 0 * * *", ("2024-01-01", "2024-01-05"), 4, "2024-01-01", "2024-01-02"),
        (
            "0 * * * *",
            ("2024-01-01T00:00:00Z", "2024-01-01T03:00:00Z"),
            3,
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
        ),
        ("0 0 * * *", ("2024-01-01", "2024-01-01"), 0, None, None),
        ("0 0 * * *", ("2024-01-05", "2024-01-01"), 0, None, None),
        ("0 0 1 * *", ("2024-01-01", "2024-05-01"), 4, "2024-01-01", "2024-02-01"),
    ],
    ids=["daily", "hourly", "zero-length", "inverted", "monthly-variable"],
)
def test_iter_intervals(
    cron: str,
    overall: Tuple[str, str],
    expected_count: int,
    first_start: Optional[str],
    first_end: Optional[str],
) -> None:
    iv = _iv(*overall)
    intervals = list(iter_intervals(cron, iv))
    assert len(intervals) == expected_count
    if first_start is not None:
        assert intervals[0] == (
            ensure_pendulum_datetime_utc(first_start),
            ensure_pendulum_datetime_utc(first_end),
        )


def test_monthly_variable_length_intervals() -> None:
    """Monthly cron produces intervals of variable length (28-31 days)."""
    overall = _iv("2024-01-01", "2024-05-01")
    intervals = list(iter_intervals("0 0 1 * *", overall))
    lengths = [(iv[1] - iv[0]).days for iv in intervals]
    assert lengths == [31, 29, 31, 30]


def test_weekly_cron() -> None:
    """Weekly cron (every Monday at midnight)."""
    overall = _iv("2024-01-01", "2024-01-29")
    intervals = list(iter_intervals("0 0 * * 1", overall))
    assert len(intervals) == 4
    for iv in intervals:
        assert (iv[1] - iv[0]).days == 7


def test_iter_intervals_is_lazy() -> None:
    overall = _iv("2024-01-01", "2024-12-31")
    gen = iter_intervals("0 0 * * *", overall)
    first = next(gen)
    assert first == (
        ensure_pendulum_datetime_utc("2024-01-01"),
        ensure_pendulum_datetime_utc("2024-01-02"),
    )
    second = next(gen)
    assert second == (
        ensure_pendulum_datetime_utc("2024-01-02"),
        ensure_pendulum_datetime_utc("2024-01-03"),
    )


def test_eligible_intervals_skips_completed() -> None:
    completed = sort_and_coalesce(
        [
            (
                ensure_pendulum_datetime_utc("2024-01-01"),
                ensure_pendulum_datetime_utc("2024-01-02"),
            ),
            (
                ensure_pendulum_datetime_utc("2024-01-02"),
                ensure_pendulum_datetime_utc("2024-01-03"),
            ),
        ]
    )
    overall = _iv("2024-01-01", "2024-01-05")
    eligible = get_eligible_intervals("0 0 * * *", overall, completed)
    assert len(eligible) == 2
    assert eligible[0][0] == ensure_pendulum_datetime_utc("2024-01-03")


def test_eligible_intervals_all_when_none_completed() -> None:
    overall = _iv("2024-01-01", "2024-01-05")
    eligible = get_eligible_intervals("0 0 * * *", overall, [])
    assert len(eligible) == 4


def test_eligible_intervals_ordered() -> None:
    overall = _iv("2024-01-01", "2024-01-04")
    eligible = get_eligible_intervals("0 0 * * *", overall, [])
    starts = [iv[0] for iv in eligible]
    assert starts == sorted(starts)


def test_next_eligible_interval_returns_first_incomplete() -> None:
    completed = [
        (ensure_pendulum_datetime_utc("2024-01-01"), ensure_pendulum_datetime_utc("2024-01-02"))
    ]
    overall = _iv("2024-01-01", "2024-01-05")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is not None
    assert iv[0] == ensure_pendulum_datetime_utc("2024-01-02")


def test_next_eligible_interval_none_when_all_done() -> None:
    completed = [
        (ensure_pendulum_datetime_utc("2024-01-01"), ensure_pendulum_datetime_utc("2024-01-03"))
    ]
    overall = _iv("2024-01-01", "2024-01-03")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is None


def test_next_eligible_skips_leading_completed() -> None:
    """Leading completed block is trimmed, avoiding iteration over 100 done intervals."""
    completed = [
        (ensure_pendulum_datetime_utc("2024-01-01"), ensure_pendulum_datetime_utc("2024-04-10"))
    ]
    overall = _iv("2024-01-01", "2024-06-01")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is not None
    assert iv[0] == ensure_pendulum_datetime_utc("2024-04-10")


def test_next_eligible_with_gap_in_middle() -> None:
    """Completed intervals with a gap — returns the first interval in the gap."""
    completed = sort_and_coalesce(
        [
            (
                ensure_pendulum_datetime_utc("2024-01-01"),
                ensure_pendulum_datetime_utc("2024-01-03"),
            ),
            (
                ensure_pendulum_datetime_utc("2024-01-04"),
                ensure_pendulum_datetime_utc("2024-01-05"),
            ),
        ]
    )
    overall = _iv("2024-01-01", "2024-01-05")
    iv = next_eligible_interval("0 0 * * *", overall, completed)
    assert iv is not None
    assert iv == (
        ensure_pendulum_datetime_utc("2024-01-03"),
        ensure_pendulum_datetime_utc("2024-01-04"),
    )


@pytest.mark.parametrize(
    "tz,cron,overall_start,overall_end,expected_starts",
    [
        # Europe/Berlin spring forward: 2024-03-31 02:00 CET → 03:00 CEST.
        # The exact-match assertion catches the "phantom 07:00 local" duplicate tick
        # that aware-datetime cron iteration produces on DST-transition days.
        (
            "Europe/Berlin",
            "0 8 * * *",
            "2024-03-30T00:00:00Z",
            "2024-04-02T23:59:59Z",
            [
                "2024-03-30T07:00:00Z",  # pre-DST: 08:00 CET = 07:00Z
                "2024-03-31T06:00:00Z",  # post-DST: 08:00 CEST = 06:00Z — NO duplicate
                "2024-04-01T06:00:00Z",  # still CEST
            ],
        ),
        # Europe/Berlin fall back: 2024-10-27 03:00 CEST → 02:00 CET.
        # The exact match catches the "09:00 local instead of 08:00" drift that
        # aware-datetime cron iteration produces because the old CEST offset is
        # carried forward across the fold.
        (
            "Europe/Berlin",
            "0 8 * * *",
            "2024-10-26T00:00:00Z",
            "2024-10-29T23:59:59Z",
            [
                "2024-10-26T06:00:00Z",  # CEST: 08:00 = 06:00Z
                "2024-10-27T07:00:00Z",  # CET: 08:00 = 07:00Z — NOT 08:00Z (09:00 local)
                "2024-10-28T07:00:00Z",
            ],
        ),
    ],
    ids=["spring-forward", "fall-back"],
)
def test_iter_intervals_respects_tz_across_dst(
    tz: str,
    cron: str,
    overall_start: str,
    overall_end: str,
    expected_starts: List[str],
) -> None:
    """Cron ticks follow the job's wall-clock across DST; yielded intervals are UTC.

    Uses exact match on starts (not just `in`) so phantom DST duplicates or drifted
    ticks fail the test rather than being silently tolerated.
    """
    overall = _iv(overall_start, overall_end)
    intervals = list(iter_intervals(cron, overall, tz=tz))
    for iv in intervals:
        assert iv[0].tzinfo == timezone.utc
        assert iv[1].tzinfo == timezone.utc
    starts = [iv[0] for iv in intervals]
    assert starts == [ensure_pendulum_datetime_utc(s) for s in expected_starts]


@pytest.mark.parametrize(
    "tz,cron,overall_start,overall_end,expected_start,expected_end",
    [
        # overall[0] in Berlin CEST is 06-01T02:00; next "0 0" Berlin is 06-02T00:00 = 06-01T22:00Z
        (
            "Europe/Berlin",
            "0 0 * * *",
            "2024-06-01T00:00:00Z",
            "2024-06-03T00:00:00Z",
            "2024-06-01T22:00:00Z",
            "2024-06-02T22:00:00Z",
        ),
        # NY winter (EST -05:00): "0 0" NY = 05:00Z
        (
            "America/New_York",
            "0 0 * * *",
            "2024-01-01T00:00:00Z",
            "2024-01-03T00:00:00Z",
            "2024-01-01T05:00:00Z",
            "2024-01-02T05:00:00Z",
        ),
    ],
    ids=["berlin-summer", "new-york-winter"],
)
def test_next_eligible_interval_tz_returns_utc(
    tz: str,
    cron: str,
    overall_start: str,
    overall_end: str,
    expected_start: str,
    expected_end: str,
) -> None:
    """next_eligible_interval with a non-UTC tz returns UTC intervals."""
    overall = _iv(overall_start, overall_end)
    iv = next_eligible_interval(cron, overall, [], tz=tz)
    assert iv is not None
    assert iv[0].tzinfo == timezone.utc
    assert iv == (
        ensure_pendulum_datetime_utc(expected_start),
        ensure_pendulum_datetime_utc(expected_end),
    )
