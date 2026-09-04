from typing import Any, Dict, List, Literal, Optional

from dlt.common.typing import Annotated, Doc, NotRequired, TypedDict

from dlt._workspace.deployment.typing import TJobResult
from dlt._workspace.typing import TWorkspaceAccess, TWorkspaceLocalVerb


AGENT_MODEL_ALIASES: Dict[str, str] = {
    "sonnet": "anthropic:claude-sonnet-5",
    "opus": "anthropic:claude-opus-5",
    "haiku": "anthropic:claude-haiku-4-5",
    "fable": "anthropic:claude-fable-5",
    "gpt": "openai:gpt-5.5",
    "gpt-mini": "openai:gpt-5.4-mini",
    "gpt-nano": "openai:gpt-5.4-nano",
    "gemini": "google:gemini-3.5-flash",
    "gemini-pro": "google:gemini-3.1-pro-preview",
}
"""Shorthands for `provider:model`, the naming pydantic-ai uses and dlt follows."""

TAgentJobStatus = Literal["succeeded", "failed", "aborted"]
"""Outcome an agent reports. `aborted` means it could not start the task at all."""


class TAgentLimits(TypedDict, total=False):
    """Loop budget. The manifest declares it, and the runtime supplies the limit that applies."""

    max_turns: int
    max_tokens: int


class TAgentDefaults(TypedDict, total=False):
    """Settings the manifest suggests and the runtime may override."""

    trigger: List[str]
    model: str
    limits: TAgentLimits
    loop_run_args: Dict[str, Any]


class TAgentSpec(TypedDict):
    """An `AGENT.md` in full: frontmatter plus the body that is the system prompt."""

    name: str
    description: NotRequired[str]
    access: NotRequired[TWorkspaceAccess]
    """What the agent asks to touch. The job definition carries it to the runtime, which grants
    it or does not."""
    inputs: Dict[str, Any]
    """JSON Schema of the inputs, substituted into the system prompt placeholders."""
    output: Dict[str, Any]
    system_prompt: str
    """The body, `{{ }}` placeholders included. Rendered against the inputs at run time."""
    tools: NotRequired[List[str]]
    skills: NotRequired[List[str]]
    rules: NotRequired[List[str]]
    defaults: NotRequired[TAgentDefaults]


class TAgentSettings(TypedDict):
    """What the runtime decided for one run."""

    loop_type: str
    model: str
    instructions: Optional[str]
    """The user turn. Absent, the loop sends a bare go-signal and the system prompt speaks alone."""
    max_turns: Optional[int]
    max_tokens: Optional[int]
    loop_run_args: Dict[str, Any]
    verbosity: int
    api_key: Optional[str]
    api_url: Optional[str]
    api_version: Optional[str]
    endpoint_source: str
    """Whose model and credentials the run uses: `"user"` or `"runtime"`."""
    trace_url: Optional[str]
    trace_key: Optional[str]
    workspace_root: str


class TAgentToolUse(TypedDict):
    """One tool call the model made."""

    name: str
    """Tool name as the model called it."""
    kind: Literal["builtin", "mcp", "skill"]
    server: NotRequired[str]
    """MCP server the tool belongs to."""


class TAgentTurn(TypedDict):
    tools: List[TAgentToolUse]
    input_tokens: int
    output_tokens: int


class TAgentTrace(TypedDict):
    """Everything the loop ran with, and what it did, read after the loop completes."""

    agent: str
    agent_file: str
    loop_type: str
    model: str
    limits: TAgentLimits
    loop_run_args: Dict[str, Any]
    instructions: str
    """The user turn this run sent."""
    inputs: Dict[str, Any]
    access: TWorkspaceAccess
    local_tools: Dict[str, TWorkspaceLocalVerb]
    """Local tools the loop wired, by the verb that bought each: `Grep` under `read`."""
    mcp_features: List[str]
    native_skills: List[str]
    inlined_skills: List[str]
    unresolved_placeholders: List[str]
    turn_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    turns: List[TAgentTurn]
    tools_used: List[str]
    skills_used: List[str]
    mcp_tools_used: List[str]
    cost_usd: NotRequired[float]
    stop_reason: NotRequired[str]
    ignored_loop_run_args: NotRequired[List[str]]


class TAgentOutput(TypedDict):
    """What an agent returns: `status`, `summary` and whatever its own output declares."""

    status: Annotated[
        TAgentJobStatus,
        Doc(
            "Outcome of your task. `succeeded` and `failed` mean what your system prompt says"
            " they mean. `aborted`: you hit something that prevents doing the task at all, and"
            " the runner raises an exception carrying `summary`."
        ),
    ]
    summary: Annotated[
        str,
        Doc(
            "Markdown. What you accomplished. When `status` is `aborted` this becomes the"
            " exception text, so say what blocked you."
        ),
    ]


class TAgentJobResult(TJobResult):
    """Job result of an agent run: the outcome lifted out of `result`, plus the loop's trace."""

    status: TAgentJobStatus
    summary: str
    trace: NotRequired[TAgentTrace]
