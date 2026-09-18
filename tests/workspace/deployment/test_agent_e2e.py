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
from dlt.common.utils import uniq_id

from dlt._workspace.deployment.agent.typing import TAgentJobResult
from dlt._workspace.deployment.configuration import AgentConfiguration
from dlt._workspace.deployment.launchers import (
    LAUNCHER_AGENT,
    LOOP_CLAUDE_AGENT_SDK,
    LOOP_PYDANTIC_AI,
)
from dlt._workspace.deployment.launchers.agent import run as agent_run
from dlt._workspace.deployment.typing import TJobRef, TRuntimeEntryPoint

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


@pytest.fixture
def workspace() -> Iterator[Any]:
    # the test secrets stand in for `~/.dlt`, so the launcher resolves `jobs.agent.*` on its own
    with importable_workspace(
        "agent_e2e_workspace", JOBS_MODULE, global_dir=TEST_SETTINGS_DIR
    ) as ctx:
        if not resolve_configuration(AgentConfiguration(), sections=("jobs",)).effective_api_key:
            pytest.skip("jobs.agent.api_key is not configured")
        yield ctx


def _entry(job: str, loop_type: str) -> TRuntimeEntryPoint:
    return {
        "module": JOBS_MODULE,
        "function": job,
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef(f"jobs.{JOBS_MODULE}.{job}"),
        "config": {"agent": {"loop": loop_type}},
    }


def _names(values: Iterable[str]) -> Set[str]:
    """Names as the test compares them: lowercase, without an MCP server or toolkit prefix."""
    return {str(v).strip().lower().rpartition("__")[2].rpartition(":")[2] for v in values}


def _markers(values: Iterable[str]) -> Set[str]:
    """The marker tokens in the report, however the model wrapped them."""
    return set(MARKER.findall(" ".join(str(v) for v in values)))


def _seen(output: TAgentJobResult) -> str:
    """What the model said and did, for the message of a failing assertion."""
    trace = output["trace"]
    return json.dumps(
        {
            "summary": output.get("summary"),
            "result": output.get("result"),
            "tools_used": trace["tools_used"],
            "mcp_tools_used": trace["mcp_tools_used"],
            "turns": trace["turns"],
        },
        pretty=True,
    )


@pytest.mark.parametrize(
    "loop_type", [LOOP_PYDANTIC_AI, LOOP_CLAUDE_AGENT_SDK], ids=["pydantic-ai", "claude"]
)
@pytest.mark.parametrize(
    "job,type_name",
    [("self_report_md", "e2e:self-report"), ("self_report_py", f"{JOBS_MODULE}:self_report_py")],
    ids=["md", "py"],
)
def test_agent_reports_what_dlt_wired(
    workspace: Any, loop_type: str, job: str, type_name: str
) -> None:
    output = agent_run(_entry(job, loop_type), run_id=uniq_id(), trigger="manual:")
    seen = _seen(output)
    trace = output["trace"]
    on_claude = loop_type == LOOP_CLAUDE_AGENT_SDK

    assert output["status"] == "succeeded", seen
    assert output["type"] == f"job.background_agent.{type_name}"
    assert trace["loop_type"] == loop_type

    # dlt's side: what `local: read` bought, and what the server was told to serve
    read_tools = {"Read", "Glob", "Grep"} | ({"NotebookRead"} if on_claude else set())
    assert trace["local_tools"] == {tool: "read" for tool in read_tools}
    assert trace["mcp_features"] == ["workspace"]
    assert trace["native_skills" if on_claude else "inlined_skills"] == ["e2e:visible-skill"]
    # the task forbids the file tools: with them the hidden markers are one grep away
    assert not _names(trace["tools_used"]) & (READ_TOOLS | UNGRANTED_TOOLS), seen
    assert "get_workspace_info" in _names(trace["mcp_tools_used"]), seen
    if on_claude:
        # the skill's marker is behind the `Skill` tool, so reporting it means loading it
        assert "visible-skill" in _names(trace["skills_used"]), seen

    # the model's side: what it saw. On pydantic-ai nothing in a name tells an MCP tool from a
    # local one, so the two buckets are read together; a tool the model never needed may go
    # unlisted, a tool the server pruned must not appear
    report = output["result"]
    tools = _names(report["local_tools"]) | _names(report["mcp_tools"])
    assert READ_TOOLS | {"get_workspace_info"} <= tools, seen
    assert not UNGRANTED_TOOLS & tools, seen
    if on_claude:
        assert SERVED_MCP_TOOLS <= _names(report["mcp_tools"]), seen
    skills = _names(report["skills"])
    assert "visible-skill" in skills and "hidden-skill" not in skills, seen
    markers = _markers(report["markers"])
    assert VISIBLE_MARKERS <= markers, seen
    assert not HIDDEN_MARKERS & markers, seen
    # the CLI loads the project's `CLAUDE.md`; the other loop has no such file
    assert ("MARKER-CLAUDE-MD" in markers) == on_claude, seen
    assert report["workspace_name"] == workspace.name, seen
