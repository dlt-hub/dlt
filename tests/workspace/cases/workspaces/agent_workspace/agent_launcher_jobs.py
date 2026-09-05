"""Agent jobs the launcher tests run. Not exported from `__deployment__`."""

import asyncio
from typing import Any, Dict, List, Literal

import dlt
from dlt.common.typing import NotRequired

from dlt.hub.run import TAgentOutput, TAgentSpec, TJobRunContext, agent

import mock_loop  # noqa: F401
from mock_loop import MOCK_LOOP


INLINE_AGENT: TAgentSpec = {
    "name": "inline-inspector",
    "description": "Declared in the deployment module instead of installed with a toolkit.",
    "access": {"data": ["read"]},
    "inputs": {
        "type": "object",
        "properties": {
            "failed_run_id": {"type": "string"},
            "depth": {"type": "integer"},
        },
        "required": ["failed_run_id"],
    },
    "output": {
        "type": "object",
        "properties": {"status": {"type": "string"}, "summary": {"type": "string"}},
    },
    "system_prompt": "You inspect runs. Inspect '{{ failed_run_id }}' at depth {{ depth }}.",
}


AGENT_REF = "dlthub-platform:job-inspector"

inspector = agent(
    AGENT_REF,
    name="mock_inspector",
    loop=MOCK_LOOP,
    instructions="explain the failure",
)


@agent(agent=AGENT_REF, loop=MOCK_LOOP)
def context_aware(run_context: TJobRunContext = None) -> Dict[str, Any]:
    return {
        "run_id": run_context["run_id"],
        "trigger": run_context["trigger"],
        "interval_start": run_context.get("interval_start"),
        "run_args": run_context.get("run_args"),
    }


@agent(agent=AGENT_REF, loop=MOCK_LOOP)
def no_context() -> str:
    return "no_context_ok"


@agent(agent=AGENT_REF, loop=MOCK_LOOP)
async def async_job(run_context: TJobRunContext = None) -> str:
    return "async_ok"


inline = agent(INLINE_AGENT, loop=MOCK_LOOP)


@agent(agent=AGENT_REF, loop=MOCK_LOOP)
def driver(run_context: TJobRunContext = None) -> Dict[str, Any]:
    """Drives the loop the launcher built for it."""
    loop = run_context["ai_loop"]
    return asyncio.run(loop.run({"failed_run_id": "r-driven"}))  # type: ignore[no-any-return]


class CrashReport(TAgentOutput):
    classification: Literal["config", "code", "upstream_data"]
    confidence: Literal["high", "medium", "low"]
    evidence: NotRequired[List[str]]


@agent(
    loop=MOCK_LOOP,
    access={"data": ["read"], "context": ["read"]},
    tools=["telemetry"],
    model="haiku",
)
def python_inspector(
    failed_run_id: str = dlt.config.value, depth: int = 2, run_context: TJobRunContext = None
) -> CrashReport:
    """Inspects a failed run without any AGENT.md.

    You run unattended and report a diagnosis with evidence.
    Investigate run '{{ failed_run_id }}' at depth {{ depth }}.
    """
    loop = run_context["ai_loop"]
    return asyncio.run(  # type: ignore[no-any-return]
        loop.run({"failed_run_id": failed_run_id, "depth": depth})
    )


@agent(loop=MOCK_LOOP, access={"data": "read", "context": "read"})
def sanity_check(run_context: TJobRunContext = None) -> TAgentOutput:
    """Sanity check agent. Lists the skills, tools and MCP tools it can see."""
    return asyncio.run(run_context["ai_loop"].run())  # type: ignore[no-any-return]
