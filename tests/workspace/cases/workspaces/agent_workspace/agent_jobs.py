"""Agent jobs used by the deployment tests."""

import signal
from typing import Any, Dict

import dlt
from dlt.hub.run import TJobRunContext, agent

import mock_loop  # noqa: F401
from mock_loop import MOCK_LOOP


def extend_inputs(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Adds an input the trigger did not carry."""
    return {"failed_job_ref": inputs.get("failed_job_ref") or "jobs.batch.ingest"}


@agent(
    agent="dlthub-platform:job-inspector", loop=MOCK_LOOP, model="haiku", identity="ignored_for_now"
)
def inspect_crash(run_context: TJobRunContext = None) -> Dict[str, Any]:
    """Drives the loop itself."""
    return {"status": "succeeded", "summary": "drove the loop by hand"}


@agent(agent="dlthub-platform:job-inspector", loop=MOCK_LOOP)
def interval_aware(run_context: TJobRunContext = None) -> Dict[str, Any]:
    """Reports what the launcher set up around the call."""
    current = dlt.current.interval()
    return {
        "ctx_start": run_context["interval_start"].isoformat(),
        "current_start": current[0].isoformat(),
        "sigint_handler": signal.getsignal(signal.SIGINT),
    }


inspector = agent(
    "dlthub-platform:job-inspector",
    loop=MOCK_LOOP,
    instructions="I expect incremental on github actions traces to generate dupes",
    trigger="0 7 * * *",
    require={"timezone": "Europe/Berlin"},
    inputs_validator=extend_inputs,
)

watcher = agent(
    "dlthub-platform:job-inspector", loop=MOCK_LOOP, name="watcher", trigger="job.fail:*"
)

tag_watcher = agent(
    "dlthub-platform:job-inspector",
    loop=MOCK_LOOP,
    name="tag_watcher",
    trigger="job.fail:tag:ingest",
)
