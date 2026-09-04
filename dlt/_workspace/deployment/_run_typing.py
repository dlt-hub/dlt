"""Run-orchestration types — shared between local and remote `run` / `serve`."""

from typing import Any, Dict, List, Literal

from dlt.common.typing import NotRequired, TypedDict


TRunLocation = Literal["local", "remote"]

TAgentEventKind = Literal[
    "start", "prompt", "turn", "thinks", "says", "tool_call", "tool_result", "mcp", "finish"
]


class TAgentEvent(TypedDict):
    """One step of an agent run, as it happens."""

    kind: TAgentEventKind
    agent: str
    text: NotRequired[str]
    """What the agent said, thought, or was asked."""
    tool: NotRequired[str]
    server: NotRequired[str]
    """MCP server the tool belongs to."""
    detail: NotRequired[Any]
    """Tool arguments on a call, the returned value on a result."""
    error: NotRequired[bool]
    """The tool failed."""
    turn: NotRequired[int]
    input_tokens: NotRequired[int]
    output_tokens: NotRequired[int]
    model: NotRequired[str]
    limits: NotRequired[str]
    status: NotRequired[str]
    total_tokens: NotRequired[int]
    cost_usd: NotRequired[float]
    tools: NotRequired[List[str]]
    """Distinct builtin tools the run called, on `finish`."""
    skills: NotRequired[List[str]]
    mcp_tools: NotRequired[List[str]]


class TRunJobInfo(TypedDict):
    """Resolved `workspace run` request — all data needed to launch the job."""

    job_ref: str
    display_label: str
    trigger: str
    trigger_humanized: str
    launcher: str
    run_id: str
    entry_point: Dict[str, Any]
    manifest_warnings: List[str]
    refresh_warning: NotRequired[str]
    profile_warning: NotRequired[str]


class TRunBannerInfo(TypedDict):
    """Data shown in the unified `Starting <job> [local|remote] ...` banner."""

    display_label: str
    job_ref: str
    trigger: str
    trigger_humanized: str
    profile: str
    location: TRunLocation
    run_id: NotRequired[str]
    workspace_name: NotRequired[str]
    port: NotRequired[int]
