"""Agent loop on the Claude Agent SDK."""

import dataclasses
from collections import deque
from typing import Any, ClassVar, Deque, Dict, List, Optional, Tuple, cast

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException

from dlt._workspace.deployment.agent.loop import AgentLoop, distinct_tools_used, split_model_id
from dlt._workspace.deployment.agent.loops.tools import (
    LOCAL_TOOL_VERBS,
    LOCAL_TOOLS,
    MCP_SERVER_ID,
    mcp_server_command,
    secret_deny_rules,
    temp_dir,
)
from dlt._workspace.deployment.agent.manifest import (
    granted,
    inline_components,
    resolve_component_ref,
)
from dlt._workspace.deployment.agent.exceptions import (
    AgentComponentNotFound,
    AgentRunFailed,
    UnsupportedAgentModel,
)
from dlt._workspace.deployment.agent.typing import (
    TAgentLimits,
    TAgentSpec,
    TAgentToolUse,
    TAgentTurn,
)
from dlt._workspace.deployment.launchers import LOOP_CLAUDE_AGENT_SDK, LOOP_PYDANTIC_AI
from dlt._workspace.deployment.reflection import model_schema
from dlt._workspace.typing import TWorkspaceAccess, TWorkspaceLocalVerb

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        ClaudeSDKError,
        ResultMessage,
        SystemMessage,
        UserMessage,
    )
    from claude_agent_sdk.types import TextBlock, ThinkingBlock, ToolResultBlock, ToolUseBlock
except ModuleNotFoundError as ex:
    raise MissingDependencyException(
        "dlt background agents on claude-agent-sdk",
        ["claude-agent-sdk"],
        "Install it directly, or in a dlt checkout run `uv sync --group agent-claude`.",
    ) from ex


AI_LOOP_TOOLS: Dict[str, Tuple[str, ...]] = {
    "Read": ("Read", "NotebookRead"),
    "Glob": ("Glob",),
    "Grep": ("Grep",),
    "Write": ("Write",),
    "Edit": ("Edit", "MultiEdit", "NotebookEdit"),
    # the last two only read and stop a shell the agent left running in the background
    "Bash": ("Bash", "PowerShell", "BashOutput", "KillShell"),
    "RunPython": (),
    "WebFetch": ("WebFetch",),
    "WebSearch": ("WebSearch",),
}
"""CLI tools behind each name in `LOCAL_TOOLS`. Python runs through `Bash` here."""
CLI_STDERR_KEPT = 20
"""CLI stderr lines held back, to explain a run that ends without a result."""

MCP_TOOL_PREFIX = "mcp__"
MCP_TOOL_PATTERN = f"{MCP_TOOL_PREFIX}{MCP_SERVER_ID}__*"
SKILL_TOOL_NAME = "Skill"


def classify_tool(name: str, tool_input: Optional[Dict[str, Any]] = None) -> TAgentToolUse:
    """Sorts a tool call into an MCP tool, a skill, or a CLI builtin."""
    if name.startswith(MCP_TOOL_PREFIX):
        parts = name[len(MCP_TOOL_PREFIX) :].split("__", 1)
        use: TAgentToolUse = {"name": parts[-1], "kind": "mcp"}
        if len(parts) == 2:
            use["server"] = parts[0]
        return use
    if name == SKILL_TOOL_NAME:
        # the CLI names the invoked skill in the tool input, under one of these keys
        named = tool_input or {}
        skill = named.get("skill") or named.get("name") or named.get("command") or name
        return {"name": str(skill), "kind": "skill"}
    return {"name": name, "kind": "builtin"}


class ClaudeAgentSdkLoop(AgentLoop):
    """The loop runs the Claude Code CLI, with a shell, a filesystem and native skills."""

    LOOP_TYPE: ClassVar[str] = LOOP_CLAUDE_AGENT_SDK
    DEFAULT_MODEL: ClassVar[str] = "sonnet"
    DEFAULT_PROVIDER: ClassVar[str] = "anthropic"
    DEFAULT_MAX_TURNS: ClassVar[Optional[int]] = 30
    DEFAULT_MAX_TOKENS: ClassVar[Optional[int]] = None

    def __init__(self, settings: Any) -> None:
        super().__init__(settings)
        self._client: Any = None
        self._options: Any = None
        self._tool_names: Dict[str, str] = {}
        self._ai_loop_tools: List[str] = []
        self._reads_project_settings: bool = False
        self._cli_stderr: Deque[str] = deque(maxlen=CLI_STDERR_KEPT)

    @property
    def native(self) -> Any:
        return self._client

    def init(self, agent_spec: TAgentSpec) -> None:
        self.spec = agent_spec
        workspace_root = self.settings["workspace_root"]
        self._reads_project_settings = bool((agent_spec.get("access") or {}).get("toolkits"))
        verbs = granted(agent_spec, "local")
        self._ai_loop_tools = [
            tool
            for verb, names in LOCAL_TOOLS.items()
            if verb in verbs
            for name in names
            for tool in AI_LOOP_TOOLS[name]
        ]

        # rules have no equivalent in any framework; skills load natively when installed
        parts = [
            agent_spec["system_prompt"],
            *inline_components(agent_spec.get("rules") or [], "rule", workspace_root),
        ]
        for ref in agent_spec.get("skills") or []:
            try:
                resolve_component_ref(ref, "skill", workspace_root)
            except AgentComponentNotFound:
                self._inlined_skills.append(ref)
            else:
                # the CLI finds a skill in the project settings, which toolkits open
                if self._reads_project_settings:
                    self._native_skills.append(ref)
                else:
                    self._inlined_skills.append(ref)
        if self._inlined_skills:
            parts += inline_components(self._inlined_skills, "skill", workspace_root)
        # the harness names its own file tools, so the prompt is where the model learns this
        parts.append(
            f"The workspace is `{workspace_root}`. Scratch files belong in the temp folder"
            f" `{temp_dir()}`."
        )
        self._system_prompt = "\n\n".join(parts)

    def local_tools(self) -> Dict[str, TWorkspaceLocalVerb]:
        """The CLI tools on the allowlist, each under the verb of the name it extends."""
        by_cli_tool = {
            cli_tool: LOCAL_TOOL_VERBS[name]
            for name, cli_tools in AI_LOOP_TOOLS.items()
            for cli_tool in cli_tools
        }
        return {tool: by_cli_tool[tool] for tool in self._ai_loop_tools}

    def _ai_loop_model(self) -> str:
        """The bare model name the CLI takes. It runs Anthropic models only."""
        provider, name = split_model_id(self.model_id())
        if provider != self.DEFAULT_PROVIDER:
            raise UnsupportedAgentModel(
                self.LOOP_TYPE,
                self.model_id(),
                f"the Claude Code CLI runs {self.DEFAULT_PROVIDER} models. Run this one on"
                f" {LOOP_PYDANTIC_AI!r}",
            )
        return name

    def _build_options(self, system_prompt: str) -> Any:
        # the CLI subprocess inherits os.environ, where the launcher put the workspace profile
        env: Dict[str, str] = {}
        if self.settings.get("api_key"):
            env["ANTHROPIC_API_KEY"] = self.settings["api_key"]
        if self.settings.get("api_url"):
            env["ANTHROPIC_BASE_URL"] = self.settings["api_url"]

        known = {f.name for f in dataclasses.fields(ClaudeAgentOptions)}
        extra = {k: v for k, v in self.settings["loop_run_args"].items() if k in known}
        self._ignored_run_args = sorted(k for k in self.settings["loop_run_args"] if k not in known)

        # with toolkits the project settings bring their own servers, ours included
        servers: Dict[str, Any] = {}
        tools = list(self.spec.get("tools") or [])
        if tools and not self._reads_project_settings:
            servers[MCP_SERVER_ID] = mcp_server_command(tools, self.spec.get("access") or {})
        allowed = list(self._ai_loop_tools)
        if servers or self._reads_project_settings:
            allowed.append(MCP_TOOL_PATTERN)
        return ClaudeAgentOptions(
            system_prompt=system_prompt,
            model=self._ai_loop_model(),
            # `tools` is what exists at all; `allowed_tools` only says what runs unprompted
            tools=self._ai_loop_tools,
            allowed_tools=allowed,
            # the CLI owns its file tools, so credential files are kept out by rule
            disallowed_tools=secret_deny_rules(),
            permission_mode="dontAsk",
            # an empty list hides every skill the CLI would otherwise offer on its own
            skills=[r.rpartition(":")[2] for r in self._native_skills],
            output_format={"type": "json_schema", "schema": model_schema(self.spec["output"])},
            max_turns=self.settings["max_turns"],
            setting_sources=["project"] if self._reads_project_settings else [],
            stderr=self._log_cli_stderr,
            mcp_servers=servers,
            strict_mcp_config=not self._reads_project_settings,
            env=env,
            cwd=self.settings["workspace_root"],
            # the CLI's file tools stop at the working directory unless a folder is added to it
            add_dirs=[str(temp_dir()), *extra.pop("add_dirs", [])],
            **extra,
        )

    async def run(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        run_args: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        limits: Optional[TAgentLimits] = None,
        instructions: Optional[str] = None,
    ) -> Dict[str, Any]:
        inputs = inputs or {}
        self.resolve_run(model, limits, instructions)
        if run_args:
            self.settings["loop_run_args"] = {**self.settings["loop_run_args"], **run_args}
        self._cli_stderr.clear()
        self._options = self._build_options(self.render_system_prompt(inputs))
        self._client = ClaudeSDKClient(options=self._options)

        self.emit_run_start(self.user_turn)
        turns: List[TAgentTurn] = []
        result: Any = None
        try:
            async with self._client:
                await self._client.query(self.user_turn)
                async for message in self._client.receive_response():
                    if isinstance(message, SystemMessage):
                        self._emit_servers(message)
                    elif isinstance(message, AssistantMessage):
                        self._emit_message(message, len(turns) + 1)
                        turns.append(_turn_from_message(message))
                        # over the limit this raises, and leaving the client block ends the CLI
                        self.count_tokens(turns[-1]["input_tokens"], turns[-1]["output_tokens"])
                    elif isinstance(message, UserMessage):
                        self._emit_tool_results(message)
                    elif isinstance(message, ResultMessage):
                        result = message
        except ClaudeSDKError as ex:
            # the CLI's stderr went to our callback, so the SDK's message says only "check stderr"
            raise AgentRunFailed(self.LOOP_TYPE, self.agent_ref, self._cli_failure(str(ex))) from ex
        if result is None:
            raise AgentRunFailed(self.LOOP_TYPE, self.agent_ref, self._cli_failure())
        self._trace = self._build_trace(inputs, result, turns)
        self.emit_run_finished((result.structured_output or {}).get("status"))
        if result.is_error:
            raise AgentRunFailed(
                self.LOOP_TYPE, self.agent_ref, str(result.errors or result.terminal_reason)
            )
        return result.structured_output  # type: ignore[no-any-return]

    def _cli_failure(self, message: str = "no result message was returned") -> str:
        """Why the CLI died: what it wrote on the way out, after the framework's own message."""
        return "; ".join([message, *self._cli_stderr])

    def _log_cli_stderr(self, line: str) -> None:
        """The CLI writes its own notices to our stderr. They belong in the log instead."""
        self._cli_stderr.append(line)
        logger.debug(f"[{self.log_name}] {line}")

    def _emit_servers(self, message: Any) -> None:
        """Reports the MCP servers the CLI connected, as its init message lists them."""
        if message.subtype != "init":
            return
        for server in message.data.get("mcp_servers") or []:
            self.emit("mcp", text=f"{server.get('name')} {server.get('status')}")

    def _emit_message(self, message: Any, turn: int) -> None:
        """Reports what the model said, thought and called in one assistant message."""
        try:
            usage = message.usage or {}
            self.emit(
                "turn",
                turn=turn,
                input_tokens=usage.get("input_tokens", 0),
                output_tokens=usage.get("output_tokens", 0),
            )
            for block in message.content:
                if isinstance(block, ThinkingBlock):
                    self.emit("thinks", text=block.thinking)
                elif isinstance(block, TextBlock):
                    self.emit("says", text=block.text)
                elif isinstance(block, ToolUseBlock):
                    self._tool_names[block.id] = block.name
                    used = classify_tool(block.name, block.input)
                    self.emit(
                        "tool_call",
                        tool=used["name"],
                        server=used.get("server"),
                        detail=block.input,
                    )
        except Exception as ex:
            # a run must never fail because it could not be reported
            logger.debug(f"Could not report an assistant message: {ex}")

    def _emit_tool_results(self, message: Any) -> None:
        """Reports what each tool handed back, named after the call it answers."""
        try:
            if isinstance(message.content, str):
                return
            for block in message.content:
                if isinstance(block, ToolResultBlock):
                    name = self._tool_names.get(block.tool_use_id, "tool")
                    self.emit(
                        "tool_result",
                        tool=classify_tool(name)["name"],
                        detail=block.content,
                        error=bool(block.is_error),
                    )
        except Exception as ex:
            # a run must never fail because it could not be reported
            logger.debug(f"Could not report a tool result: {ex}")

    def _build_trace(self, inputs: Dict[str, Any], result: Any, turns: List[TAgentTurn]) -> Any:
        trace = self._base_trace(inputs)
        usage = result.usage or {}
        # num_turns counts sub-agent turns too, so it can exceed the assistant messages seen
        trace["turn_count"] = result.num_turns or len(turns)
        trace["input_tokens"] = usage.get("input_tokens", 0)
        trace["output_tokens"] = usage.get("output_tokens", 0)
        trace["total_tokens"] = trace["input_tokens"] + trace["output_tokens"]
        trace["turns"] = turns
        trace["tools_used"], trace["skills_used"], trace["mcp_tools_used"] = distinct_tools_used(
            turns
        )
        if result.total_cost_usd is not None:
            trace["cost_usd"] = result.total_cost_usd
        if result.terminal_reason:
            trace["stop_reason"] = result.terminal_reason
        if self._ignored_run_args:
            trace["ignored_loop_run_args"] = self._ignored_run_args
        return trace


def _turn_from_message(message: Any) -> TAgentTurn:
    usage = message.usage or {}
    return {
        "tools": [
            classify_tool(b.name, b.input) for b in message.content if isinstance(b, ToolUseBlock)
        ],
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
    }
