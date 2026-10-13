"""Agent jobs whose agents ship an `agent.py`. Not exported from `__deployment__`."""

from typing import Any, Dict, List

from dlt.hub.run import agent

import mock_loop  # noqa: F401
from mock_loop import MOCK_LOOP

SEEN: List[Dict[str, Any]] = []
"""What the job's own validators received, after the agent's code ran."""


def job_inputs(inputs: Dict[str, Any]) -> Dict[str, Any]:
    SEEN.append({"inputs": dict(inputs)})
    return {**inputs, "failed_run_id": f"{inputs['failed_run_id']}+job"}


def job_output(output: Dict[str, Any]) -> Dict[str, Any]:
    SEEN.append({"output": dict(output)})
    return {**output, "summary": "refined by the job"}


checked = agent("dlthub-platform:checked-inspector", loop=MOCK_LOOP)

checked_twice = agent(
    "dlthub-platform:checked-inspector",
    name="checked_twice",
    loop=MOCK_LOOP,
    inputs_validator=job_inputs,
    outputs_validator=job_output,
)

broken = agent("dlthub-platform:broken-code", loop=MOCK_LOOP)
