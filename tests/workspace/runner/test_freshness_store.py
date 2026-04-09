"""Tests for the per-job freshness store implementation."""

from dlt.common.pendulum import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from dlt._workspace._runner.freshness_store import DuckDBJobFreshnessStore


def _dt(s: str) -> pendulum.DateTime:
    return ensure_pendulum_datetime_utc(s)


def test_get_unknown_returns_none() -> None:
    store = DuckDBJobFreshnessStore()
    assert store.get_prev_completed_run("jobs.never.set") is None
    store.close()


def test_set_then_get_round_trips() -> None:
    store = DuckDBJobFreshnessStore()
    ts = _dt("2024-06-15T10:30:00Z")
    store.set_prev_completed_run("jobs.a", ts)
    assert store.get_prev_completed_run("jobs.a") == ts
    store.close()


def test_set_overwrites_previous_value() -> None:
    store = DuckDBJobFreshnessStore()
    store.set_prev_completed_run("jobs.a", _dt("2024-06-15T10:00:00Z"))
    store.set_prev_completed_run("jobs.a", _dt("2024-06-15T11:00:00Z"))
    assert store.get_prev_completed_run("jobs.a") == _dt("2024-06-15T11:00:00Z")
    store.close()


def test_clear_returns_none() -> None:
    store = DuckDBJobFreshnessStore()
    store.set_prev_completed_run("jobs.a", _dt("2024-06-15T10:30:00Z"))
    store.clear_prev_completed_run("jobs.a")
    assert store.get_prev_completed_run("jobs.a") is None
    store.close()


def test_set_none_clears() -> None:
    store = DuckDBJobFreshnessStore()
    store.set_prev_completed_run("jobs.a", _dt("2024-06-15T10:30:00Z"))
    store.set_prev_completed_run("jobs.a", None)
    assert store.get_prev_completed_run("jobs.a") is None
    store.close()


def test_clear_unknown_is_noop() -> None:
    store = DuckDBJobFreshnessStore()
    store.clear_prev_completed_run("jobs.never.set")
    assert store.get_prev_completed_run("jobs.never.set") is None
    store.close()


def test_multiple_jobs_isolated() -> None:
    store = DuckDBJobFreshnessStore()
    store.set_prev_completed_run("jobs.a", _dt("2024-06-15T10:00:00Z"))
    store.set_prev_completed_run("jobs.b", _dt("2024-06-15T11:00:00Z"))
    assert store.get_prev_completed_run("jobs.a") == _dt("2024-06-15T10:00:00Z")
    assert store.get_prev_completed_run("jobs.b") == _dt("2024-06-15T11:00:00Z")
    # clearing one does not affect the other
    store.clear_prev_completed_run("jobs.a")
    assert store.get_prev_completed_run("jobs.a") is None
    assert store.get_prev_completed_run("jobs.b") == _dt("2024-06-15T11:00:00Z")
    store.close()


def test_returned_datetime_is_utc() -> None:
    store = DuckDBJobFreshnessStore()
    store.set_prev_completed_run("jobs.a", _dt("2024-06-15T10:30:00Z"))
    result = store.get_prev_completed_run("jobs.a")
    assert result is not None
    assert result.tzname() == "UTC"
    store.close()
