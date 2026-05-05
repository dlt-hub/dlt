"""Tests for DuckDB interval store implementation."""

from typing import List, Tuple

import pytest

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from tests.workspace.runner._runner.interval_store import DuckDBIntervalStore


def _dt(s: str) -> pendulum.DateTime:
    return ensure_pendulum_datetime_utc(s)


@pytest.mark.parametrize(
    "completed,query_start,query_end,expected",
    [
        ([("2024-01-01", "2024-01-02")], "2024-01-01", "2024-01-02", True),
        (
            [("2024-01-01", "2024-01-02"), ("2024-01-04", "2024-01-05")],
            "2024-01-01",
            "2024-01-05",
            False,
        ),
        (
            [("2024-01-01", "2024-01-03"), ("2024-01-03", "2024-01-05")],
            "2024-01-01",
            "2024-01-05",
            True,
        ),
        ([("2024-01-01", "2024-01-03")], "2024-01-01", "2024-01-05", False),
        ([], "2024-01-01", "2024-01-02", False),
        (
            [("2024-01-01", "2024-01-04"), ("2024-01-03", "2024-01-06")],
            "2024-01-01",
            "2024-01-06",
            True,
        ),
    ],
    ids=["exact", "gap", "adjacent", "partial", "empty", "overlapping"],
)
def test_store_is_range_covered(
    completed: List[Tuple[str, str]],
    query_start: str,
    query_end: str,
    expected: bool,
) -> None:
    store = DuckDBIntervalStore()
    for s, e in completed:
        store.mark_interval_completed("jobs.a", (_dt(s), _dt(e)))
    assert store.is_interval_completed("jobs.a", (_dt(query_start), _dt(query_end))) == expected
    store.close()


def test_store_multiple_jobs_isolated() -> None:
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-05")))
    assert not store.is_interval_completed("jobs.b", (_dt("2024-01-01"), _dt("2024-01-05")))
    store.close()


def test_daily_covers_hourly_subinterval() -> None:
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.d", (_dt("2024-01-15"), _dt("2024-01-16")))
    assert store.is_interval_completed(
        "jobs.d", (_dt("2024-01-15T08:00:00Z"), _dt("2024-01-15T09:00:00Z"))
    )
    store.close()


def test_get_completed_intervals_coalesced() -> None:
    """Adjacent and overlapping intervals are merged in output."""
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-03")))
    store.mark_interval_completed("jobs.a", (_dt("2024-01-03"), _dt("2024-01-05")))
    store.mark_interval_completed("jobs.a", (_dt("2024-01-04"), _dt("2024-01-06")))
    overall = (_dt("2024-01-01"), _dt("2024-01-10"))
    result = store.get_completed_intervals("jobs.a", overall)
    assert len(result) == 1
    assert result[0] == (_dt("2024-01-01"), _dt("2024-01-06"))
    store.close()


def test_get_completed_intervals_within_range() -> None:
    """Only intervals overlapping the requested overall are returned."""
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-05")))
    store.mark_interval_completed("jobs.a", (_dt("2024-02-01"), _dt("2024-02-05")))
    # query only covers January
    overall = (_dt("2024-01-01"), _dt("2024-01-31"))
    result = store.get_completed_intervals("jobs.a", overall)
    assert len(result) == 1
    assert result[0] == (_dt("2024-01-01"), _dt("2024-01-05"))
    store.close()


def test_invalidate_interval() -> None:
    """Invalidating a sub-range creates a gap in completed intervals."""
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-05")))
    store.invalidate_interval("jobs.a", (_dt("2024-01-02"), _dt("2024-01-03")))
    overall = (_dt("2024-01-01"), _dt("2024-01-10"))
    # the entire [Jan 1-5) row was deleted since it overlapped [Jan 2-3)
    result = store.get_completed_intervals("jobs.a", overall)
    assert len(result) == 0
    # range is no longer covered
    assert not store.is_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-05")))
    store.close()


def test_invalidate_interval_partial_overlap() -> None:
    """Invalidating removes only overlapping rows, non-overlapping stay."""
    store = DuckDBIntervalStore()
    store.mark_interval_completed("jobs.a", (_dt("2024-01-01"), _dt("2024-01-02")))
    store.mark_interval_completed("jobs.a", (_dt("2024-01-02"), _dt("2024-01-03")))
    store.mark_interval_completed("jobs.a", (_dt("2024-01-03"), _dt("2024-01-04")))
    # invalidate middle interval
    store.invalidate_interval("jobs.a", (_dt("2024-01-02"), _dt("2024-01-03")))
    overall = (_dt("2024-01-01"), _dt("2024-01-05"))
    result = store.get_completed_intervals("jobs.a", overall)
    # two remaining ranges with a gap
    assert len(result) == 2
    assert result[0] == (_dt("2024-01-01"), _dt("2024-01-02"))
    assert result[1] == (_dt("2024-01-03"), _dt("2024-01-04"))
    store.close()
