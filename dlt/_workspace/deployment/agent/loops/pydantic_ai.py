"""Agent loop on Pydantic AI."""

import functools
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, Tuple, cast

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException

from dlt._workspace.deployment.agent.loops.tools import (
    LOCAL_TOOL_VERBS,
    LOCAL_TOOLS,
    MCP_SERVER_ID,
    LocalTools,
    mcp_server_command,
    workspace_note,
)
from dlt._workspace.deployment.agent.exceptions import (
    AgentRunFailed,
    LocalToolError,
    UnsupportedAgentModel,
)
from dlt._workspace.deployment.agent.loop import AgentLoop, distinct_tools_used
from dlt._workspace.deployment.agent.manifest import granted, inline_components, inputs_schema
from dlt._workspace.deployment.agent.typing import (
    TAgentLimits,
    TAgentSpec,
    TAgentToolUse,
    TAgentTurn,
)
from dlt._workspace.deployment.launchers import LOOP_PYDANTIC_AI
from dlt._workspace.deployment.reflection import model_schema
from dlt._workspace.typing import (
    TWorkspaceAccess,
    TWorkspaceDataVerb,
    TWorkspaceLocalVerb,
    TWorkspaceContextVerb,
)

try:
    from fastmcp.client.transports import StdioTransport
    from pydantic_ai import Agent, ModelRetry, Tool, ToolFailed
    from pydantic_ai.mcp import MCPToolset
    from pydantic_ai.models import infer_model
    from pydantic_ai.native_tools import WebFetchTool, WebSearchTool
    from pydantic_ai.providers import infer_provider, infer_provider_class
    from pydantic_ai.messages import (
        ModelResponse,
        PartEndEvent,
        RetryPromptPart,
        TextPart,
        ToolResultEvent,
        ThinkingPart,
        ToolCallPart,
    )
    from pydantic_ai.exceptions import UnexpectedModelBehavior
    from pydantic_ai.usage import UsageLimits
except ModuleNotFoundError as ex:
    raise MissingDependencyException(
        "dlthub background agents on pydantic-ai",
        ["pydantic-ai-slim[anthropic,openai,google,mcp,spec]"],
    ) from ex


OUTPUT_TOOL_NAME = "final_result"
"""How pydantic-ai names the tool that carries the answer."""

GATEWAY_PREFIX = "gateway/"

DEFAULT_URL_ARG = "base_url"
DEFAULT_KEY_ARG = "api_key"

PROVIDER_URL_ARG: Dict[str, Optional[str]] = {
    "azure": "azure_endpoint",
    "azure-responses": "azure_endpoint",
    "litellm": "api_base",
    "xai": "api_host",
    # these reach one endpoint of their own and take no url; `openrouter` has `app_url`,
    # which is the attribution header and not the endpoint
    "cerebras": None,
    "cohere": None,
    "crusoe": None,
    "deepseek": None,
    "fireworks": None,
    "github": None,
    "moonshotai": None,
    "nebius": None,
    "openrouter": None,
    "ovhcloud": None,
    "together": None,
    "vercel": None,
    "voyageai": None,
    "zai": None,
}
"""What each provider calls `api_url` in its constructor. `base_url` where it is not listed"""

PROVIDER_KEY_ARG: Dict[str, str] = {"snowflake": "token"}
"""What each provider calls `api_key` in its constructor. `api_key` where it is not listed."""

PROVIDER_VERSION_ARG: Dict[str, str] = {"azure": "api_version", "azure-responses": "api_version"}
"""Providers that version their API. Azure is the only one pydantic-ai gives the argument to."""

NATIVE_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    "network": {"WebSearch": WebSearchTool, "WebFetch": WebFetchTool},
}
"""Verbs the provider serves itself: web access. `execute` stays in the workspace, on the
platform's shell tool and `RunPython`."""

NATIVE_CAPABILITY_SPECS: Dict[str, Dict[str, Any]] = {
    "WebSearch": {"WebSearch": {}},
    "WebFetch": {"WebFetch": {}},
}
"""How each native tool is written in an `AgentSpec` `capabilities` list."""


def _as_tool_function(fn: Callable[..., str], retries: int) -> Callable[..., str]:
    """A local tool whose errors pydantic-ai either retries within a budget or hands over as failed."""
    error_cls = ModelRetry if retries else ToolFailed

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> str:
        try:
            return fn(*args, **kwargs)
        except LocalToolError as ex:
            raise error_cls(str(ex)) from ex

    return wrapper


def _answer_text(answer: Dict[str, Any]) -> str:
    """What the agent said when it answered: its summary, or the whole answer."""
    return str(answer.get("summary") or answer)


def _failure_reason(ex: Exception) -> str:
    """The framework's reason, plus where the limit lives when it names one."""
    reason = str(ex)
    if "retries" in reason:
        reason += " In dlt that limit is `loop_run_args.retries` in the agent defaults."
    return reason


def make_local_tools(tools: LocalTools, verbs: Set[str], retries: int = 0) -> List[Any]:
    """pydantic-ai `Tool`s for the local verbs the agent declared. No verb, no tool.

    With `retries` at 0 a tool error is a failed call the model sees and moves on from; above
    it, pydantic-ai asks the model to correct the call, that many times per tool.
    """
    served = tools.by_name()
    return [
        Tool(_as_tool_function(served[name], retries), name=name)
        for verb, names in LOCAL_TOOLS.items()
        if verb in verbs
        for name in names
        if name in served
    ]


class PydanticAILoop(AgentLoop):
    """Pydantic AI has no shell, so it enforces access at the tool surface, not the process."""

    LOOP_TYPE: ClassVar[str] = LOOP_PYDANTIC_AI
    DEFAULT_MODEL: ClassVar[str] = "sonnet"
    DEFAULT_PROVIDER: ClassVar[str] = "anthropic"
    DEFAULT_MAX_TURNS: ClassVar[Optional[int]] = 50
    DEFAULT_MAX_TOKENS: ClassVar[Optional[int]] = None

    def __init__(self, settings: Any) -> None:
        super().__init__(settings)
        self._agent: Any = None
        self._tools: LocalTools = None
        self._local_tools: Set[str] = set()
        self._native_tools: Set[str] = set()
        self._turn: int = 0
        self._input_seen: int = 0
        self._output_seen: int = 0

    @property
    def native(self) -> Any:
        return self._agent

    @property
    def tool_retries(self) -> int:
        """`loop_run_args.retries`: how often the model may correct a failing tool call.

        0, the default, hands every tool error to the model as a failed call instead: the run
        goes on, and `max_turns` is what bounds it.
        """
        return int(self.settings["loop_run_args"].get("retries") or 0)

    def init(self, agent_spec: TAgentSpec) -> None:
        self.spec = agent_spec
        workspace_root = self.settings["workspace_root"]
        # neither rules nor skills exist here, so both are inlined into the system prompt
        self._inlined_skills = list(agent_spec.get("skills") or [])
        self._tools = LocalTools(workspace_root)
        self._system_prompt = "\n\n".join(
            [
                agent_spec["system_prompt"],
                *inline_components(agent_spec.get("rules") or [], "rule", workspace_root),
                *inline_components(self._inlined_skills, "skill", workspace_root),
                workspace_note(self._tools.root, self._tools.scratch),
            ]
        )

    def _build_agent(self, system_prompt: str) -> Any:
        model = self._build_model()
        agent = Agent.from_spec(
            self._agent_spec_dict(model),
            model=model,
            tools=self._build_tools(),
            toolsets=self._build_toolsets(),
        )
        # `AgentSpec.instructions` is a handlebars template whenever `deps_schema` is set, and
        # dlt has already rendered the prompt: a `{{ }}` left in a rule, a skill or an example
        # would be silently dropped. A function is taken verbatim.
        agent.instructions(lambda ctx: system_prompt)
        return agent

    def _agent_spec_dict(self, model: Any) -> Dict[str, Any]:
        """The manifest as an `AgentSpec`. The system prompt is attached to the agent itself."""
        spec_dict: Dict[str, Any] = {
            "name": self.spec["name"],
            "deps_schema": model_schema(inputs_schema(self.spec)),
            "output_schema": model_schema(self.spec["output"]),
        }
        if description := self.spec.get("description"):
            spec_dict["description"] = description
        # loop_run_args is already AgentSpec vocabulary, so it merges without translation
        spec_dict.update(self.settings["loop_run_args"])
        if not self.tool_retries:
            # tool errors bypass the budget then; pydantic-ai keeps its own default for the
            # rest of it (output validation, protocol errors), which 0 would end at first sight
            spec_dict.pop("retries", None)
        verbs = granted(self.spec, "local")
        served = type(model).supported_native_tools()
        native_names = [
            name
            for verb, tools in NATIVE_CAPABILITIES.items()
            if verb in verbs
            for name, tool in tools.items()
            if tool in served
        ]
        self._native_tools = set(native_names)
        native: List[Any] = [NATIVE_CAPABILITY_SPECS[name] for name in native_names]
        if native:
            capabilities: List[Any] = list(spec_dict.get("capabilities") or [])
            spec_dict["capabilities"] = capabilities + native
        return spec_dict

    def _build_tools(self) -> List[Any]:
        """Local tools the declaration permits."""
        tools = make_local_tools(self._tools, granted(self.spec, "local"), self.tool_retries)
        self._local_tools = {tool.name for tool in tools}
        return tools

    def _build_toolsets(self) -> List[Any]:
        """The workspace MCP server, limited to the tools the granted access covers."""
        tools = list(self.spec.get("tools") or [])
        if not tools:
            # an agent that asks for no feature group gets no server to ask
            return []
        server = mcp_server_command(tools, self.spec.get("access") or {})
        transport = StdioTransport(
            command=server["command"],
            args=server["args"],
            env=server["env"],
            cwd=self.settings["workspace_root"],
        )
        return [
            MCPToolset(
                transport,
                id=MCP_SERVER_ID,
                tool_error_behavior="retry" if self.tool_retries else "failed",
            )
        ]

    def local_tools(self) -> Dict[str, TWorkspaceLocalVerb]:
        """The function tools built for the declaration, and the provider's own the model serves."""
        wired = self._local_tools | self._native_tools
        verbs = {
            **LOCAL_TOOL_VERBS,
            **{name: verb for verb, tools in NATIVE_CAPABILITIES.items() for name in tools},
        }
        return {name: cast(TWorkspaceLocalVerb, verbs[name]) for name in verbs if name in wired}

    def _build_model(self) -> Any:
        """The model this run names, on a provider carrying the configured key and url."""
        return infer_model(self.model_id(), provider_factory=self._build_provider)

    def _build_provider(self, name: str) -> Any:
        """The named provider, carrying the configured key and url. Its env vars fill the rest."""
        try:
            provider_cls = infer_provider_class(name)
        except ImportError as ex:
            raise MissingDependencyException(
                f"dlt background agents on {name} models", [f"pydantic-ai-slim[{name}]"], str(ex)
            ) from ex
        except ValueError as ex:
            raise UnsupportedAgentModel(self.LOOP_TYPE, self.model_id(), str(ex)) from ex

        args: Dict[str, Any] = {}
        if key := self.settings.get("api_key"):
            args[PROVIDER_KEY_ARG.get(name, DEFAULT_KEY_ARG)] = key
        if url := self.settings.get("api_url"):
            url_arg = PROVIDER_URL_ARG.get(name, DEFAULT_URL_ARG)
            if url_arg is None:
                raise UnsupportedAgentModel(
                    self.LOOP_TYPE,
                    self.model_id(),
                    f"provider {name!r} calls its own endpoint and takes no api_url. Drop"
                    " `agent.api_url`, or name a provider that takes one, such as `openai`",
                )
            args[url_arg] = url
        if version := self.settings.get("api_version"):
            version_arg = PROVIDER_VERSION_ARG.get(name)
            if version_arg is None:
                # an unused version is harmless, unlike a url that sends the run elsewhere
                logger.warning(f"Provider {name!r} takes no api_version; {version!r} is ignored.")
            else:
                args[version_arg] = version
        if not args:
            return infer_provider(name)
        if name.startswith(GATEWAY_PREFIX):
            # the gateway routes to an upstream provider, so it is built by name, not by class
            from pydantic_ai.providers.gateway import gateway_provider

            return gateway_provider(name[len(GATEWAY_PREFIX) :], **args)
        return provider_cls(**args)

    async def run(
        self,
        instructions: Optional[str] = None,
        inputs: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        limits: Optional[TAgentLimits] = None,
        run_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        inputs = inputs or {}
        self.resolve_run(model, limits, instructions)
        # the usage offsets belong to one run, like the counters `resolve_run` starts over
        self._input_seen = self._output_seen = 0
        if run_args:
            self.settings["loop_run_args"] = {**self.settings["loop_run_args"], **run_args}
        self._agent = self._build_agent(self.render_system_prompt(inputs))

        # tokens are counted by the loop, turn by turn, the same way on every framework
        usage_limits = UsageLimits(request_limit=self.settings["max_turns"])
        self.emit_run_start(self.user_turn)
        self._turn = 0
        async with self._agent:
            if self.spec.get("tools"):
                self.emit("mcp", text=f"{MCP_SERVER_ID} connected")
            try:
                result = await self._agent.run(
                    self.user_turn,
                    deps=inputs,
                    usage_limits=usage_limits,
                    event_stream_handler=self._emit_events,
                )
            except UnexpectedModelBehavior as ex:
                raise AgentRunFailed(self.LOOP_TYPE, self.agent_ref, _failure_reason(ex)) from ex
        self._trace = self._build_trace(inputs, result)
        self.emit_run_finished(result.output.get("status"))
        return result.output  # type: ignore[no-any-return]

    async def _emit_events(self, ctx: Any, events: Any) -> None:
        """Reports what the model says, thinks and calls while it runs. One call, one turn."""
        self._turn += 1
        # the run context's usage is cumulative through the previous turn, so this is where
        # that turn's tokens are known; over the limit, the raise ends the run before this one
        usage = ctx.usage
        self.count_tokens(
            usage.input_tokens - self._input_seen, usage.output_tokens - self._output_seen
        )
        self._input_seen, self._output_seen = usage.input_tokens, usage.output_tokens
        self.emit("turn", turn=self._turn)
        async for event in events:
            try:
                if isinstance(event, PartEndEvent):
                    part = event.part
                    if isinstance(part, ThinkingPart):
                        self.emit("thinks", text=part.content)
                    elif isinstance(part, TextPart):
                        self.emit("says", text=part.content)
                    elif isinstance(part, ToolCallPart):
                        if part.tool_name == OUTPUT_TOOL_NAME:
                            self.emit("says", text=_answer_text(part.args_as_dict()))
                        else:
                            self.emit(
                                "tool_call",
                                tool=part.tool_name,
                                server=(
                                    None if part.tool_name in self._local_tools else MCP_SERVER_ID
                                ),
                                detail=part.args_as_dict(),
                            )
                elif isinstance(event, ToolResultEvent):
                    if event.part.tool_name == OUTPUT_TOOL_NAME:
                        continue
                    self.emit(
                        "tool_result",
                        tool=event.part.tool_name,
                        detail=event.part.content,
                        error=(
                            isinstance(event.part, RetryPromptPart)
                            or getattr(event.part, "outcome", None) == "failed"
                        ),
                    )
            except Exception as ex:
                # a run must never fail because it could not be reported
                logger.debug(f"Could not report agent event {type(event).__name__}: {ex}")

    def _build_trace(self, inputs: Dict[str, Any], result: Any) -> Any:
        trace = self._base_trace(inputs)
        # `usage` was a method before pydantic-ai 2.36 and is a property from it on
        usage = result.usage
        if callable(usage):
            usage = usage()
        trace["turn_count"] = usage.requests
        trace["input_tokens"] = usage.input_tokens
        trace["output_tokens"] = usage.output_tokens
        trace["total_tokens"] = usage.total_tokens
        turns: List[TAgentTurn] = []
        for message in result.all_messages():
            if not isinstance(message, ModelResponse):
                continue
            turns.append(
                {
                    "tools": [
                        self._tool_use(p.tool_name)
                        for p in message.parts
                        if isinstance(p, ToolCallPart) and p.tool_name != OUTPUT_TOOL_NAME
                    ],
                    "input_tokens": message.usage.input_tokens,
                    "output_tokens": message.usage.output_tokens,
                }
            )
        trace["turns"] = turns
        trace["tools_used"], trace["skills_used"], trace["mcp_tools_used"] = distinct_tools_used(
            turns
        )
        return trace

    def _tool_use(self, name: str) -> TAgentToolUse:
        """A function tool is the loop's own; every other name the model called is the server's."""
        if name in self._local_tools:
            return {"name": name, "kind": "builtin"}
        return {"name": name, "kind": "mcp", "server": MCP_SERVER_ID}
