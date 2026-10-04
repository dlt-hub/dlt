"""Agent runs on real loops and real models: four model calls, run by `make test-workspace-agents`.

The agent reports its own setup, and the test holds the report against what dlt wired: the
local tools the access bought, the MCP tools the server kept, the declared skill, the declared
rule's marker, and the result of one MCP tool call. Skipped when `jobs.agent.api_key` is not
configured.
"""

import re
from typing import Any, Iterable, Iterator, Set

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.json import json

from dlt._workspace.deployment.configuration import AgentConfiguration
from dlt.hub.run import TAgentJobResult, TJobRunContext

from tests.workspace.utils import TEST_SETTINGS_DIR, importable_workspace

JOBS_MODULE = "e2e_agents"
READ_TOOLS = {"read", "glob", "grep"}
SERVED_MCP_TOOLS = {"get_workspace_info", "list_profiles"}
# what `local: read` does not buy, in the names the loops give the model
UNGRANTED_TOOLS = {
    "write",
    "edit",
    "multiedit",
    "notebookedit",
    "bash",
    "bashoutput",
    "killshell",
    "powershell",
    "runpython",
    "webfetch",
    "websearch",
    "list_pipelines",
}
VISIBLE_MARKERS = {"MARKER-PROMPT-CONTROL", "MARKER-RULE-VISIBLE", "MARKER-SKILL-VISIBLE"}
HIDDEN_MARKERS = {"MARKER-RULE-HIDDEN", "MARKER-SKILL-HIDDEN"}
MARKER = re.compile(r"MARKER-[A-Z]+(?:-[A-Z]+)*")
FAILED_RUN_ID = "r-failed-42"


@pytest.fixture
def workspace() -> Iterator[Any]:
    # the test secrets stand in for `~/.dlt`, so the launcher resolves `jobs.agent.*` on its own
    with importable_workspace(
        "agent_e2e_workspace", JOBS_MODULE, global_dir=TEST_SETTINGS_DIR
    ) as ctx:
        if not resolve_configuration(AgentConfiguration(), sections=("jobs",)).effective_api_key:
            pytest.skip("jobs.agent.api_key is not configured")
        yield ctx


def _normalize_names(values: Iterable[str]) -> Set[str]:
    """Names as the test compares them: lowercase, without an MCP server or toolkit prefix."""
    return {str(v).strip().lower().rpartition("__")[2].rpartition(":")[2] for v in values}


def _extract_markers(values: Iterable[str]) -> Set[str]:
    """The marker tokens in the report, however the model wrapped them."""
    return set(MARKER.findall(" ".join(str(v) for v in values)))


def _format_run_for_failure(job_result: TAgentJobResult) -> str:
    """What the model said and did, for the message of a failing assertion."""
    trace = job_result["trace"]
    return json.dumps(
        {
            "summary": job_result.get("summary"),
            "result": job_result.get("result"),
            "tools_used": trace["tools_used"],
            "mcp_tools_used": trace["mcp_tools_used"],
            "turns": trace["turns"],
        },
        pretty=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "loop_type", ["pydantic-ai", "claude-agent-sdk"], ids=["pydantic-ai", "claude"]
)
@pytest.mark.parametrize(
    "job,type_name",
    [("self_report_md", "e2e:self-report"), ("self_report_py", f"{JOBS_MODULE}:self_report_py")],
    ids=["md", "py"],
)
async def test_agent_reports_what_dlt_wired(
    workspace: Any, monkeypatch: pytest.MonkeyPatch, loop_type: str, job: str, type_name: str
) -> None:
    import e2e_agents  # type: ignore[import-not-found]

    monkeypatch.setenv("JOBS__AGENT__LOOP", loop_type)
    agent_job = getattr(e2e_agents, job)
    on_claude = loop_type == "claude-agent-sdk"

    # inputs go as keyword arguments; an explicit run context is used as given
    run_context: TJobRunContext = {
        "run_id": "r-e2e",
        "trigger": "manual:e2e-test",  # type: ignore[typeddict-item]
        "refresh": False,
    }
    # the call returns the agent output
    report = await agent_job(failed_run_id=FAILED_RUN_ID, run_context=run_context)
    # the job result the launcher would deliver, with the agent trace, kept on the job
    job_result = agent_job.last_job_result
    trace = job_result["trace"]
    run_details = _format_run_for_failure(job_result)

    assert report["status"] == "succeeded", run_details
    assert job_result["status"] == "succeeded"
    assert job_result["type"] == f"job.background_agent.{type_name}"
    assert job_result["result"] == report
    # the agent saw the input and the run context it was given
    assert report["reported_run_id"] == FAILED_RUN_ID, run_details
    assert report["trigger"] == "manual:e2e-test", run_details
    assert trace["inputs"]["failed_run_id"] == FAILED_RUN_ID
    assert trace["inputs"]["run_context"]["run_id"] == "r-e2e"
    # entity-typed inputs and outputs name what the run acted on
    assert job_result["object"] == [
        {"type": "job-runs", "id": f"job-runs/{FAILED_RUN_ID}"},
        {"type": "workspace", "id": f"workspace/{workspace.name}"},
    ]

    # dlt's side: what `local: read` bought, and what the server was told to serve
    assert trace["loop_type"] == loop_type
    read_tools = {"Read", "Glob", "Grep"} | ({"NotebookRead"} if on_claude else set())
    assert trace["local_tools"] == {tool: "read" for tool in read_tools}
    assert trace["mcp_features"] == ["workspace"]
    assert trace["native_skills" if on_claude else "inlined_skills"] == ["e2e:visible-skill"]
    # the task forbids the file tools: with them the hidden markers are one grep away
    assert not _normalize_names(trace["tools_used"]) & (READ_TOOLS | UNGRANTED_TOOLS), run_details
    assert "get_workspace_info" in _normalize_names(trace["mcp_tools_used"]), run_details
    if on_claude:
        # the skill's marker is behind the `Skill` tool, so reporting it means loading it
        assert "visible-skill" in _normalize_names(trace["skills_used"]), run_details

    # the model's side: what it saw. On pydantic-ai nothing in a name tells an MCP tool from a
    # local one, so the two buckets are read together; a tool the model never needed may go
    # unlisted, a tool the server pruned must not appear
    tools = _normalize_names(report["local_tools"]) | _normalize_names(report["mcp_tools"])
    assert READ_TOOLS | {"get_workspace_info"} <= tools, run_details
    assert not UNGRANTED_TOOLS & tools, run_details
    if on_claude:
        assert SERVED_MCP_TOOLS <= _normalize_names(report["mcp_tools"]), run_details
    skills = _normalize_names(report["skills"])
    assert "visible-skill" in skills and "hidden-skill" not in skills, run_details
    markers = _extract_markers(report["markers"])
    assert VISIBLE_MARKERS <= markers, run_details
    assert not HIDDEN_MARKERS & markers, run_details
    # the CLI loads the project's `CLAUDE.md`; the other loop has no such file
    assert ("MARKER-CLAUDE-MD" in markers) == on_claude, run_details
    assert report["workspace_name"] == workspace.name, run_details


@pytest.mark.asyncio
@pytest.mark.parametrize("job", ["self_report_md", "self_report_py"], ids=["md", "py"])
async def test_agent_gets_a_run_context_when_none_is_given(
    workspace: Any, monkeypatch: pytest.MonkeyPatch, job: str
) -> None:
    import e2e_agents

    monkeypatch.setenv("JOBS__AGENT__LOOP", "pydantic-ai")
    agent_job = getattr(e2e_agents, job)

    report = await agent_job(failed_run_id=FAILED_RUN_ID)
    run_context = agent_job.last_job_result["trace"]["inputs"]["run_context"]

    # a direct call is a manual run
    assert run_context["trigger"] == "manual:"
    assert run_context["run_id"]
    assert report["trigger"] == "manual:", _format_run_for_failure(agent_job.last_job_result)
