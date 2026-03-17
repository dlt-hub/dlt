"""Tests for the trigger scheduler."""

import time
from typing import List

import pytest

from dlt._workspace._runner.scheduler import TriggerScheduler
from dlt._workspace.deployment._trigger_helpers import filter_jobs_by_selectors, matches_selector
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecutionSpec,
    TJobDefinition,
    TJobRef,
    TTrigger,
)


def _job(ref: str, triggers: List[str]) -> TJobDefinition:
    return {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(module="m", function="f", job_type="batch"),
        "triggers": [TTrigger(t) for t in triggers],
        "execution": TExecutionSpec(),
        "starred": False,
    }


def test_immediate_triggers() -> None:
    """http, deployment, manual, and tag fire immediately."""
    sched = TriggerScheduler()
    job = _job("jobs.mod.web", ["http:", "deployment:", "tag:foo", "manual:jobs.mod.web"])
    immediate = sched.register_job(job)
    assert len(immediate) == 4


def test_manual_trigger_fires_immediately() -> None:
    """Manual triggers always fire immediately."""
    sched = TriggerScheduler()
    j = _job("jobs.mod.manual_job", ["manual:jobs.mod.manual_job"])
    immediate = sched.register_job(j)
    assert len(immediate) == 1


def test_every_fires_immediately_and_schedules() -> None:
    """every trigger fires immediately and enters future schedule."""
    sched = TriggerScheduler(with_future=True)
    job = _job("jobs.mod.poll", ["every:1s"])
    immediate = sched.register_job(job)

    assert len(immediate) == 1
    assert not sched.is_empty()
    assert sched.get_next_fire_time() is not None


def test_every_without_future_no_schedule() -> None:
    """every trigger fires immediately but nothing scheduled without --with-future."""
    sched = TriggerScheduler(with_future=False)
    job = _job("jobs.mod.poll", ["every:1s"])
    immediate = sched.register_job(job)

    assert len(immediate) == 1
    assert sched.is_empty()


def test_job_event_triggers() -> None:
    """job.success/job.fail triggers register as event-based, fire on event."""
    sched = TriggerScheduler()
    upstream = _job("jobs.mod.up", [])
    downstream = _job("jobs.mod.down", ["job.success:jobs.mod.up"])

    sched.register_job(upstream)
    sched.register_job(downstream)

    # firing wrong event returns nothing
    assert sched.fire_event("job.fail:jobs.mod.up") == []

    # firing correct event returns downstream
    triggered = sched.fire_event("job.success:jobs.mod.up")
    assert len(triggered) == 1
    assert triggered[0][0]["job_ref"] == "jobs.mod.down"

    # event triggers are persistent — fires again on repeat
    triggered2 = sched.fire_event("job.success:jobs.mod.up")
    assert len(triggered2) == 1
    assert triggered2[0][0]["job_ref"] == "jobs.mod.down"


def test_pop_due_jobs() -> None:
    """Timed jobs fire when their time arrives."""
    sched = TriggerScheduler(with_future=True)
    job = _job("jobs.mod.poll", ["every:0.01s"])

    # every fires immediately AND schedules next at now+period
    immediate = sched.register_job(job)
    assert len(immediate) == 1

    # wait for the scheduled item to become due
    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) >= 1


def test_repeated_timed_job_fires_followup_each_time() -> None:
    """Followup fires every time the timed upstream completes, not just once."""
    sched = TriggerScheduler(with_future=True)
    upstream = _job("jobs.mod.poll", ["every:0.01s"])
    downstream = _job("jobs.mod.process", ["job.success:jobs.mod.poll"])

    from dlt._workspace._runner.scheduler import EVENT_TYPES

    sched.register_job(upstream)
    sched.register_job(downstream, only=EVENT_TYPES)

    # first cycle: every fires immediately
    # simulate upstream completion
    triggered = sched.fire_event("job.success:jobs.mod.poll")
    assert len(triggered) == 1
    assert triggered[0][0]["job_ref"] == "jobs.mod.process"

    # wait for timed re-fire
    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) >= 1

    # second cycle: upstream completes again, followup fires again
    triggered2 = sched.fire_event("job.success:jobs.mod.poll")
    assert len(triggered2) == 1
    assert triggered2[0][0]["job_ref"] == "jobs.mod.process"


def test_with_future_once_no_repeat() -> None:
    """With --with-future-once, every trigger doesn't re-enter schedule."""
    sched = TriggerScheduler(with_future_once=True)
    job = _job("jobs.mod.poll", ["every:0.01s"])
    sched.register_job(job)

    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) >= 1

    # should not re-enter
    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) == 0
    assert sched.is_empty()


def test_with_future_once_warning() -> None:
    """One-shot items produce a warning after firing."""
    sched = TriggerScheduler(with_future_once=True)
    job = _job("jobs.mod.poll", ["every:0.01s"])
    sched.register_job(job)

    time.sleep(0.05)
    sched.pop_due_jobs()
    warnings = sched.pop_warnings()
    assert len(warnings) >= 1
    assert "will not run again" in warnings[0]


def test_empty_scheduler() -> None:
    sched = TriggerScheduler()
    assert sched.is_empty()
    assert sched.get_next_fire_time() is None


def test_filter_jobs_by_selectors() -> None:
    jobs = [
        _job("jobs.a", ["tag:backfill"]),
        _job("jobs.b", ["http:"]),
        _job("jobs.c", ["schedule:0 8 * * *"]),
    ]

    # no selectors = all jobs
    assert len(filter_jobs_by_selectors(jobs, [])) == 3

    # filter by tag
    filtered = filter_jobs_by_selectors(jobs, ["tag:*"])
    assert len(filtered) == 1
    assert filtered[0]["job_ref"] == "jobs.a"

    # filter by multiple selectors
    filtered = filter_jobs_by_selectors(jobs, ["tag:*", "http:*"])
    assert len(filtered) == 2
