"""Tests for the trigger scheduler."""

import time
from typing import List, Tuple

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


def _register(
    sched: TriggerScheduler, job: TJobDefinition
) -> List[Tuple[TJobDefinition, TTrigger]]:
    """Register all triggers from a job definition."""
    return sched.register_triggers(job, job["triggers"])


def test_immediate_triggers() -> None:
    """http, deployment, manual, and tag fire immediately."""
    sched = TriggerScheduler()
    job = _job("jobs.mod.web", ["http:", "deployment:", "tag:foo", "manual:jobs.mod.web"])
    immediate = _register(sched, job)
    assert len(immediate) == 4


def test_manual_trigger_fires_immediately() -> None:
    sched = TriggerScheduler()
    j = _job("jobs.mod.manual_job", ["manual:jobs.mod.manual_job"])
    immediate = _register(sched, j)
    assert len(immediate) == 1


def test_every_fires_immediately_and_schedules() -> None:
    """every trigger fires immediately and enters future schedule."""
    sched = TriggerScheduler(with_future=True)
    job = _job("jobs.mod.poll", ["every:1s"])
    immediate = _register(sched, job)

    assert len(immediate) == 1
    assert not sched.is_empty()
    assert sched.get_next_fire_time() is not None


def test_every_without_future_no_schedule() -> None:
    """every trigger fires immediately but nothing scheduled without --with-future."""
    sched = TriggerScheduler(with_future=False)
    job = _job("jobs.mod.poll", ["every:1s"])
    immediate = _register(sched, job)

    assert len(immediate) == 1
    assert sched.is_empty()


def test_job_event_triggers() -> None:
    """job.success/job.fail triggers register as event-based, fire on event."""
    sched = TriggerScheduler()
    downstream = _job("jobs.mod.down", ["job.success:jobs.mod.up"])

    sched.register_triggers(downstream, downstream["triggers"])

    assert sched.fire_event("job.fail:jobs.mod.up") == []

    triggered = sched.fire_event("job.success:jobs.mod.up")
    assert len(triggered) == 1
    assert triggered[0][0]["job_ref"] == "jobs.mod.down"

    # persistent — fires again
    triggered2 = sched.fire_event("job.success:jobs.mod.up")
    assert len(triggered2) == 1


def test_pop_due_jobs() -> None:
    """Timed jobs fire when their time arrives."""
    sched = TriggerScheduler(with_future=True)
    job = _job("jobs.mod.poll", ["every:0.01s"])
    immediate = _register(sched, job)
    assert len(immediate) == 1

    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) >= 1


def test_repeated_timed_job_fires_followup_each_time() -> None:
    """Followup fires every time the timed upstream completes."""
    sched = TriggerScheduler(with_future=True)
    upstream = _job("jobs.mod.poll", ["every:0.01s"])
    downstream = _job("jobs.mod.process", ["job.success:jobs.mod.poll"])

    _register(sched, upstream)
    # register only event triggers for followup
    sched.register_triggers(downstream, [TTrigger("job.success:jobs.mod.poll")])

    triggered = sched.fire_event("job.success:jobs.mod.poll")
    assert len(triggered) == 1

    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) >= 1

    # fires again
    triggered2 = sched.fire_event("job.success:jobs.mod.poll")
    assert len(triggered2) == 1


def test_with_future_once_no_repeat() -> None:
    sched = TriggerScheduler(with_future_once=True)
    job = _job("jobs.mod.poll", ["every:0.01s"])
    _register(sched, job)

    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) >= 1

    time.sleep(0.05)
    due = sched.pop_due_jobs()
    assert len(due) == 0
    assert sched.is_empty()


def test_with_future_once_warning() -> None:
    sched = TriggerScheduler(with_future_once=True)
    job = _job("jobs.mod.poll", ["every:0.01s"])
    _register(sched, job)

    time.sleep(0.05)
    sched.pop_due_jobs()
    warnings = sched.pop_warnings()
    assert len(warnings) >= 1
    assert "will not run again" in warnings[0]


def test_register_only_matched_triggers() -> None:
    """Only explicitly passed triggers are registered, not all job triggers."""
    sched = TriggerScheduler()
    job = _job("jobs.mod.x", ["manual:jobs.mod.x", "tag:daily", "job.success:jobs.mod.y"])

    # register only the tag trigger
    immediate = sched.register_triggers(job, [TTrigger("tag:daily")])
    assert len(immediate) == 1

    # manual was NOT registered
    # event was NOT registered
    assert sched.fire_event("job.success:jobs.mod.y") == []


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

    assert len(filter_jobs_by_selectors(jobs, [])) == 3

    filtered = filter_jobs_by_selectors(jobs, ["tag:*"])
    assert len(filtered) == 1
    assert filtered[0]["job_ref"] == "jobs.a"

    filtered = filter_jobs_by_selectors(jobs, ["tag:*", "http:*"])
    assert len(filtered) == 2
