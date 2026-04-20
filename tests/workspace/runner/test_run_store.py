"""Tests for DuckDB job runs store."""

from dlt.common.pendulum import pendulum

from tests.workspace.runner._runner.run_store import DuckDBJobRunsStore


def test_create_and_get_last_run() -> None:
    store = DuckDBJobRunsStore()
    now = pendulum.now("UTC")
    store.create_run(
        {
            "run_id": "r1",
            "job_ref": "jobs.a",
            "trigger": "manual:",
            "scheduled_at": now,
            "started_at": now,
            "status": "running",
        }
    )
    last = store.get_last_run("jobs.a")
    assert last is not None
    assert last["run_id"] == "r1"
    assert last["status"] == "running"
    store.close()


def test_update_run_status() -> None:
    store = DuckDBJobRunsStore()
    now = pendulum.now("UTC")
    store.create_run(
        {
            "run_id": "r1",
            "job_ref": "jobs.a",
            "trigger": "schedule:0 0 * * *",
            "scheduled_at": now,
            "started_at": now,
            "status": "running",
        }
    )
    finished = now.add(seconds=5)
    store.update_run("r1", "completed", finished_at=finished)
    last = store.get_last_run("jobs.a")
    assert last["status"] == "completed"
    assert last["finished_at"] == finished
    store.close()


def test_get_last_run_returns_most_recent() -> None:
    store = DuckDBJobRunsStore()
    t1 = pendulum.datetime(2024, 1, 1, tz="UTC")
    t2 = pendulum.datetime(2024, 1, 2, tz="UTC")
    store.create_run(
        {
            "run_id": "r1",
            "job_ref": "jobs.a",
            "trigger": "manual:",
            "scheduled_at": t1,
            "status": "completed",
        }
    )
    store.create_run(
        {
            "run_id": "r2",
            "job_ref": "jobs.a",
            "trigger": "manual:",
            "scheduled_at": t2,
            "status": "failed",
        }
    )
    last = store.get_last_run("jobs.a")
    assert last["run_id"] == "r2"
    assert last["status"] == "failed"
    store.close()


def test_get_last_run_none_when_empty() -> None:
    store = DuckDBJobRunsStore()
    assert store.get_last_run("jobs.nonexistent") is None
    store.close()


def test_jobs_isolated() -> None:
    store = DuckDBJobRunsStore()
    now = pendulum.now("UTC")
    store.create_run(
        {
            "run_id": "r1",
            "job_ref": "jobs.a",
            "trigger": "manual:",
            "scheduled_at": now,
            "status": "completed",
        }
    )
    assert store.get_last_run("jobs.b") is None
    store.close()
