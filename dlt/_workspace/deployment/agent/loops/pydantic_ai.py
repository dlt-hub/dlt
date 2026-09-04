"""Agent loop on Pydantic AI."""

import functools
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, cast

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException

from dlt._workspace.deployment.agent.loops.tools import (
    LOCAL_TOOL_VERBS,
    LOCAL_TOOLS,
    MCP_SERVER_ID,
    is_secret_file,
    mcp_server_command,
    temp_dir,
    tool_env,
)
from dlt._workspace.deployment.agent.exceptions import AgentRunFailed, UnsupportedAgentModel
from dlt._workspace.deployment.agent.loop import AgentLoop, distinct_tools_used, split_model_id
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
        "dlt background agents on pydantic-ai",
        ["pydantic-ai-slim[anthropic,openai,google,mcp,spec]"],
        "Install it directly, or in a dlt checkout run `uv sync --group agent`.",
    ) from ex


MAX_TOOL_OUTPUT = 20_000
SUBPROCESS_TIMEOUT = 120

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
"""What each provider calls `api_url` in its constructor. `base_url` where it is not listed.

Read off the pydantic-ai provider constructors: a provider that takes the url under another
name drops a `base_url` silently, and the run then talks to the provider's default endpoint.
"""

PROVIDER_KEY_ARG: Dict[str, str] = {"snowflake": "token"}
"""What each provider calls `api_key` in its constructor. `api_key` where it is not listed."""

PROVIDER_VERSION_ARG: Dict[str, str] = {"azure": "api_version", "azure-responses": "api_version"}
"""Providers that version their API. Azure is the only one pydantic-ai gives the argument to."""

NATIVE_CAPABILITIES: Dict[str, Dict[str, Any]] = {
    "network": {"WebSearch": WebSearchTool, "WebFetch": WebFetchTool},
}
"""Verbs the provider serves itself: web access. `execute` stays in the workspace, on `RunPython`."""

NATIVE_CAPABILITY_SPECS: Dict[str, Dict[str, Any]] = {
    "WebSearch": {"WebSearch": {}},
    "WebFetch": {"WebFetch": {}},
}
"""How each native tool is written in an `AgentSpec` `capabilities` list."""


def _inside(root: Path, temp: Path, path: str) -> Path:
    """Resolves a path against the workspace, refusing what escapes it and the temp folder."""
    # an absolute path replaces `root` in the join, which is how a temp file is named
    target = (root / path).resolve()
    if not (target.is_relative_to(root) or target.is_relative_to(temp)):
        raise ModelRetry(f"{path!r} is outside the workspace and the temp folder {str(temp)!r}")
    if is_secret_file(target.name):
        raise ModelRetry(f"{path!r} holds credentials and cannot be opened")
    return target


def _capture(argv: List[str], cwd: Path, stdin: str = None) -> str:
    """Runs a child process in the workspace and returns its combined, capped output."""
    try:
        done = subprocess.run(  # noqa: S603
            argv,
            input=stdin,
            cwd=str(cwd),
            env=tool_env(),
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return f"(timed out after {SUBPROCESS_TIMEOUT}s)"
    except OSError as ex:
        raise ModelRetry(f"could not run {argv[0]!r}: {ex}") from ex
    output = (done.stdout + done.stderr).strip()
    return output[:MAX_TOOL_OUTPUT] or f"(no output, exit {done.returncode})"


def _failed_not_retried(fn: Any) -> Any:
    """A tool whose `ModelRetry` reaches the model as a failed call, outside any retry budget."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except ModelRetry as ex:
            raise ToolFailed(str(ex)) from ex

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


def make_local_tools(
    workspace_root: str, verbs: Set[str], retries: int = 0, scratch_dir: Optional[str] = None
) -> List[Any]:
    """Function tools for the local verbs the agent declared. No verb, no tool.

    With `retries` at 0 a tool error is a failed call the model sees and moves on from; above
    it, pydantic-ai asks the model to correct the call, that many times per tool. The file
    tools reach the workspace and `scratch_dir`, the system temp folder unless given.

    Nothing here is a sandbox. `execute` runs in the job's own process tree and virtualenv.
    The runner that the job already runs in is what contains it.
    """
    root = Path(workspace_root).resolve()
    temp = Path(scratch_dir).resolve() if scratch_dir else temp_dir()
    tools: List[Any] = []
    where = (
        f"Path relative to the workspace root `{root}`, or an absolute path inside the temp"
        f" folder `{temp}`, where scratch files belong. Nothing else is reachable."
    )

    def read_file(path: str, offset: int = None, limit: int = None) -> str:
        """PLACEHOLDER_READ"""
        target = _inside(root, temp, path)
        if not target.is_file():
            raise ModelRetry(f"{path!r} does not exist")
        text = target.read_text(encoding="utf-8", errors="replace")
        if offset is None and limit is None:
            return text[:MAX_TOOL_OUTPUT]
        start = max((offset or 1) - 1, 0)
        lines = text.splitlines(keepends=True)[start : None if limit is None else start + limit]
        return "".join(lines)[:MAX_TOOL_OUTPUT]

    read_file.__doc__ = f"""Read a UTF-8 text file, whole or a range of lines.

        Args:
            path: {where}
            offset: First line to return, 1-based. Reads from the start when omitted.
            limit: How many lines to return. Reads to the end when omitted.
        """

    def glob_files(pattern: str = "*") -> str:
        """List workspace files matching a glob, one relative path per line.

        Args:
            pattern: Glob relative to the workspace root, e.g. `"**/*.py"`.
        """
        matches = sorted(
            str(path.relative_to(root))
            for path in root.glob(pattern)
            if path.is_file() and not is_secret_file(path.name)
        )
        return "\n".join(matches)[:MAX_TOOL_OUTPUT] or f"(nothing matches {pattern!r})"

    def grep_files(pattern: str, glob: str = "**/*") -> str:
        """Search workspace file contents, returning `path:line:text` for each hit.

        Args:
            pattern: Regular expression matched against each line.
            glob: Which files to search, relative to the workspace root.
        """
        try:
            expression = re.compile(pattern)
        except re.error as ex:
            raise ModelRetry(f"{pattern!r} is not a valid regular expression: {ex}") from ex
        hits: List[str] = []
        for path in sorted(root.glob(glob)):
            if not path.is_file() or is_secret_file(path.name):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            hits += [
                f"{path.relative_to(root)}:{number}:{line}"
                for number, line in enumerate(text.splitlines(), start=1)
                if expression.search(line)
            ]
        return "\n".join(hits)[:MAX_TOOL_OUTPUT] or f"(no line matches {pattern!r})"

    def write_file(path: str, content: str) -> str:
        """PLACEHOLDER_WRITE"""
        target = _inside(root, temp, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return f"wrote {len(content)} characters to {path}"

    write_file.__doc__ = f"""Write a UTF-8 text file, creating parent folders.

        Args:
            path: {where}
            content: Full new contents of the file. Replaces what is there.
        """

    def edit_file(path: str, old_text: str, new_text: str) -> str:
        """PLACEHOLDER_EDIT"""
        target = _inside(root, temp, path)
        if not target.is_file():
            raise ModelRetry(f"{path!r} does not exist")
        text = target.read_text(encoding="utf-8", errors="replace")
        found = text.count(old_text)
        if found != 1:
            raise ModelRetry(
                f"{old_text[:80]!r} appears {found} times in {path!r}."
                " Include more context, so it appears exactly once"
            )
        target.write_text(text.replace(old_text, new_text), encoding="utf-8")
        return f"replaced {len(old_text)} characters in {path}"

    edit_file.__doc__ = f"""Replace one fragment of a file, leaving the rest untouched.

        Args:
            path: {where}
            old_text: Text to replace. Must appear exactly once in the file.
            new_text: Text to put in its place.
        """

    def run_bash(command: str) -> str:
        """PLACEHOLDER_BASH"""
        return _capture(["bash", "-c", command], root)

    run_bash.__doc__ = f"""Run a shell command and return its stdout and stderr.

        Every call starts a fresh shell in the workspace root, `{root}`. Nothing carries
        over between calls, so `cd` in one call does not affect the next: use paths
        relative to the workspace root, or change directory inside the same command
        (`cd subdir && ls`). The job's virtualenv is first on PATH, so `dlthub`, `dlt` and
        `python` are the workspace's own.

        Args:
            command: Command line, run through `bash -c`.
        """

    def run_python(code: str) -> str:
        """PLACEHOLDER_PYTHON"""
        return _capture([sys.executable, "-"], root, stdin=code)

    run_python.__doc__ = f"""Run Python in the workspace environment and return its stdout and stderr.

        The interpreter is the workspace's own virtualenv, `{sys.executable}`, running in
        the workspace root, `{root}`: `dlt`, the workspace pipelines and every configured
        destination import exactly as they do in the job itself, under the same profile and
        credentials. Each call is a fresh process, so nothing carries over between calls and
        only what you print comes back.

        Args:
            code: Python source to execute.
        """

    # the model sees the names in LOCAL_TOOLS, whatever the function behind them is called
    served: Dict[str, Any] = {
        "Read": read_file,
        "Glob": glob_files,
        "Grep": grep_files,
        "Write": write_file,
        "Edit": edit_file,
        "Bash": run_bash,
        "RunPython": run_python,
    }
    for verb, names in LOCAL_TOOLS.items():
        if verb in verbs:
            tools += [
                Tool(served[name] if retries else _failed_not_retried(served[name]), name=name)
                for name in names
                if name in served
            ]
    return tools


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
        self._system_prompt = "\n\n".join(
            [
                agent_spec["system_prompt"],
                *inline_components(agent_spec.get("rules") or [], "rule", workspace_root),
                *inline_components(self._inlined_skills, "skill", workspace_root),
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
        tools = make_local_tools(
            self.settings["workspace_root"], granted(self.spec, "local"), self.tool_retries
        )
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
