"""The agent loop abstraction and the settings one run is assembled from."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Tuple, Type, cast

from dlt.common import json, logger
from dlt.common.configuration import plugins

from dlt._workspace.deployment.agent.exceptions import (
    AgentTokenLimitExceeded,
    AgentTraceNotAvailable,
    UnknownAgentLoop,
)
from dlt._workspace.deployment.agent.manifest import render_placeholders
from dlt._workspace.deployment.agent.typing import (
    AGENT_MODEL_ALIASES,
    TAgentLimits,
    TAgentSettings,
    TAgentSpec,
    TAgentTrace,
    TAgentTurn,
)
from dlt._workspace.deployment._run_typing import TAgentEvent, TAgentEventKind
from dlt._workspace.deployment._run_views import emit_agent_event
from dlt._workspace.deployment.configuration import AgentConfiguration
from dlt._workspace.deployment.launchers import BUILTIN_AGENT_LOOPS, DEFAULT_AGENT_LOOP
from dlt._workspace.deployment.typing import TWorkspaceAccess
from dlt._workspace.typing import TWorkspaceLocalVerb


DEFAULT_VERBOSITY = 1
"""Thoughts in one line, tool arguments and results capped. 0 shows less, 2 everything."""

DEFAULT_USER_TURN = "Begin."
"""What a run opens with when nobody gave instructions: the task is in the system prompt."""


def distinct_tools_used(turns: List[TAgentTurn]) -> Tuple[List[str], List[str], List[str]]:
    """Distinct builtin, skill and MCP names across every turn, in first-seen order."""
    seen: Dict[str, List[str]] = {"builtin": [], "skill": [], "mcp": []}
    for turn in turns:
        for use in turn["tools"]:
            bucket = seen[use["kind"]]
            if use["name"] not in bucket:
                bucket.append(use["name"])
    return seen["builtin"], seen["skill"], seen["mcp"]


def _serializable(value: Any) -> Any:
    """Coerces a value so one unserializable leaf cannot drop the whole trace."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        # the beacon client swallows serialization errors, so replace the leaf here
        pass
    if isinstance(value, Mapping):
        return {str(k): _serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serializable(v) for v in value]
    return repr(value)


class AgentLoop(ABC):
    """Runs one agent spec on one agent framework."""

    LOOP_TYPE: ClassVar[str]
    DEFAULT_MODEL: ClassVar[str] = "sonnet"
    DEFAULT_PROVIDER: ClassVar[str] = ""
    """Provider a bare model name belongs to. Empty when a loop names its models its own way."""
    DEFAULT_MAX_TURNS: ClassVar[Optional[int]] = None
    DEFAULT_MAX_TOKENS: ClassVar[Optional[int]] = None

    def __init__(self, settings: TAgentSettings) -> None:
        self.settings = settings
        self.spec: TAgentSpec = None
        self.agent_ref: str = ""
        self.agent_file: str = ""
        self._trace: TAgentTrace = None
        self._ignored_run_args: List[str] = []
        self._native_skills: List[str] = []
        self._inlined_skills: List[str] = []
        self._unresolved_placeholders: List[str] = []
        self._input_tokens: int = 0
        self._output_tokens: int = 0
        self._system_prompt: str = ""
        """Body plus whatever the loop inlines, assembled in `init` and still a template."""

    @property
    def loop_type(self) -> str:
        return self.LOOP_TYPE

    @property
    def user_turn(self) -> str:
        """What the run says to the agent. Every framework needs a first message."""
        return self.settings["instructions"] or DEFAULT_USER_TURN

    @abstractmethod
    def init(self, agent_spec: TAgentSpec) -> None:
        """Prepares the loop from the agent declaration."""

    @abstractmethod
    async def run(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        run_args: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        limits: Optional[TAgentLimits] = None,
        instructions: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Builds the native agent from the runtime arguments, runs it, returns the output.

        Args:
            inputs (Optional[Dict[str, Any]]): Declared inputs plus the implicit run context.
            run_args (Optional[Dict[str, Any]]): Arguments passed to the native loop.
            model (Optional[str]): Model for this run, overriding the declaration.
            limits (Optional[TAgentLimits]): Budget for this run, overriding the declaration.
            instructions (Optional[str]): The user turn, overriding the resolved settings.
        """

    @property
    @abstractmethod
    def native(self) -> Any:
        """The underlying framework object."""

    @property
    def trace(self) -> TAgentTrace:
        if self._trace is None:
            raise AgentTraceNotAvailable(self.LOOP_TYPE)
        return self._trace

    def resolve_run(
        self,
        model: Optional[str] = None,
        limits: Optional[TAgentLimits] = None,
        instructions: Optional[str] = None,
    ) -> None:
        """Folds this run's arguments over the resolved settings. Called first in `run`."""
        if model:
            self.settings["model"] = model
        if limits:
            if limits.get("max_turns") is not None:
                self.settings["max_turns"] = limits["max_turns"]
            if limits.get("max_tokens") is not None:
                self.settings["max_tokens"] = limits["max_tokens"]
        if instructions:
            self.settings["instructions"] = instructions
        logger.info(
            f"Agent {self.log_name} runs {self.model_id()} on the"
            f" {self.settings['endpoint_source']} endpoint."
        )

    def model_id(self) -> str:
        """Settings model as `provider:model`: aliases expanded, a bare name qualified."""
        model = self.settings["model"]
        model = AGENT_MODEL_ALIASES.get(model, model)
        if ":" in model or not self.DEFAULT_PROVIDER:
            return model
        return f"{self.DEFAULT_PROVIDER}:{model}"

    def local_tools(self) -> Dict[str, TWorkspaceLocalVerb]:
        """Local tools this loop wired, by the verb that bought each.

        `local` is the one access axis a loop serves itself; `data` and `context` are the MCP
        server's, started with the declaration. A loop without a tool for a verb wires less.
        """
        return {}

    @property
    def log_name(self) -> str:
        return self.agent_ref or (self.spec["name"] if self.spec else "agent")

    def emit(self, kind: TAgentEventKind, **fields: Any) -> None:
        """Reports one step of the run: the console when a person watches, the log otherwise."""
        event = cast(TAgentEvent, {"kind": kind, "agent": self.log_name, **fields})
        emit_agent_event(event, self.settings["verbosity"])

    def render_system_prompt(self, inputs: Mapping[str, Any]) -> str:
        """The assembled system prompt with this run's inputs substituted into its placeholders."""
        prompt, unresolved = render_placeholders(self._system_prompt, inputs)
        self._unresolved_placeholders = unresolved
        if unresolved:
            logger.warning(
                f"Agent {self.spec['name']!r} system prompt has unresolved placeholders:"
                f" {', '.join(sorted(set(unresolved)))}"
            )
        self.log_system_prompt(prompt)
        return prompt

    def log_system_prompt(self, prompt: str) -> None:
        """The assembled prompt in full: too long for INFO, kept at DEBUG."""
        logger.debug(f"[{self.log_name}] system prompt:\n{prompt}")

    def emit_run_start(self, prompt: str) -> None:
        limits = [f"max {self.settings['max_turns']} turns"] if self.settings["max_turns"] else []
        if self.settings["max_tokens"]:
            limits.append(f"{self.settings['max_tokens']:,} tokens")
        self.emit("start", model=self.model_id(), limits=" \u00b7 ".join(limits))
        self.emit("prompt", text=prompt)

    def emit_run_finished(self, status: str = None) -> None:
        trace = self.trace
        self.emit(
            "finish",
            status=status or "finished",
            turn=trace["turn_count"],
            total_tokens=trace["total_tokens"],
            cost_usd=trace.get("cost_usd"),
            tools=trace["tools_used"],
            skills=trace["skills_used"],
            mcp_tools=trace["mcp_tools_used"],
        )
        used = [
            f"{label}: {', '.join(names)}"
            for label, names in (
                ("tools", trace["tools_used"]),
                ("skills", trace["skills_used"]),
                ("mcp tools", trace["mcp_tools_used"]),
            )
            if names
        ]
        logger.info(
            f"Agent {self.log_name} finished in {trace['turn_count']} turns and"
            f" {trace['total_tokens']:,} tokens, using {'; '.join(used) or 'no tools'}."
        )

    @property
    def tokens_used(self) -> int:
        """Tokens the loop has counted so far, input and output together."""
        return self._input_tokens + self._output_tokens

    def count_tokens(self, input_tokens: int, output_tokens: int) -> None:
        """Adds one turn's tokens to the run and stops it once `max_tokens` is passed.

        Every loop reports its turns here, so the limit means the same thing on all of them.

        Raises:
            AgentTokenLimitExceeded: The run has used more tokens than it was given.
        """
        self._input_tokens += input_tokens
        self._output_tokens += output_tokens
        limit = self.settings["max_tokens"]
        if limit is not None and self.tokens_used > limit:
            raise AgentTokenLimitExceeded(self.LOOP_TYPE, self.agent_ref, self.tokens_used, limit)

    def _base_trace(self, inputs: Dict[str, Any]) -> TAgentTrace:
        """Everything the loop ran with, before the counters are filled in."""
        trace: TAgentTrace = {
            "agent": self.agent_ref,
            "agent_file": self.agent_file,
            "loop_type": self.LOOP_TYPE,
            "model": self.model_id(),
            "limits": TAgentLimits(
                max_turns=self.settings["max_turns"], max_tokens=self.settings["max_tokens"]
            ),
            "loop_run_args": dict(self.settings["loop_run_args"]),
            "inputs": _serializable(inputs),
            "access": self.spec.get("access") or {},
            "local_tools": self.local_tools(),
            "mcp_features": list(self.spec.get("tools") or []),
            "native_skills": list(self._native_skills),
            "inlined_skills": list(self._inlined_skills),
            "unresolved_placeholders": list(self._unresolved_placeholders),
            "turn_count": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "turns": [],
            "tools_used": [],
            "skills_used": [],
            "mcp_tools_used": [],
            "instructions": self.user_turn,
        }
        return trace


def resolve_agent_loop(loop_type: str) -> Type[AgentLoop]:
    """Asks plugins for a loop class answering to `loop_type`.

    Raises:
        UnknownAgentLoop: No plugin implements the requested loop.
    """
    claimed = [c for c in plugins.manager().hook.plug_agent_loop(loop_type=loop_type) if c]
    if not claimed:
        raise UnknownAgentLoop(loop_type, list(BUILTIN_AGENT_LOOPS))
    if len(claimed) > 1:
        logger.warning(
            f"{len(claimed)} plugins claim agent loop {loop_type!r}. Using"
            f" {claimed[0].__module__}.{claimed[0].__name__}"
        )
    return claimed[0]  # type: ignore[no-any-return]


def split_model_id(model: str) -> Tuple[str, str]:
    """`(provider, name)` of a model id. The provider is empty when the id carries none."""
    provider, separator, name = model.partition(":")
    return (provider, name) if separator else ("", provider)


def resolve_agent_settings(
    spec: TAgentSpec,
    config: AgentConfiguration,
    decorator_args: Mapping[str, Any],
    loop_cls: Type[AgentLoop],
    workspace_root: str,
) -> TAgentSettings:
    """Merges loop defaults, spec `defaults`, decorator arguments and config into one setting set.

    Precedence: loop class < agent spec < decorator argument < resolved config.
    """
    defaults = spec.get("defaults") or {}
    spec_limits = defaults.get("limits") or {}
    deco_limits = decorator_args.get("limits") or {}

    # `None` means "not supplied" at every layer, so overriding one limit leaves the other
    def pick(*values: Any) -> Any:
        for value in reversed(values):
            if value is not None:
                return value
        return None

    loop_run_args: Dict[str, Any] = dict(defaults.get("loop_run_args") or {})
    loop_run_args.update(decorator_args.get("loop_run_args") or {})
    loop_run_args.update(config.loop_run_args or {})

    return {
        "loop_type": loop_cls.LOOP_TYPE,
        "model": pick(
            loop_cls.DEFAULT_MODEL,
            defaults.get("model"),
            decorator_args.get("model"),
            config.effective_model,
        ),
        "instructions": pick(decorator_args.get("instructions"), config.instructions),
        "max_turns": pick(
            loop_cls.DEFAULT_MAX_TURNS,
            spec_limits.get("max_turns"),
            deco_limits.get("max_turns"),
            config.max_turns,
        ),
        "max_tokens": pick(
            loop_cls.DEFAULT_MAX_TOKENS,
            spec_limits.get("max_tokens"),
            deco_limits.get("max_tokens"),
            config.max_tokens,
        ),
        "loop_run_args": loop_run_args,
        "verbosity": pick(
            DEFAULT_VERBOSITY, None, decorator_args.get("verbosity"), config.verbosity
        ),
        "api_key": config.effective_api_key,
        "api_url": config.effective_api_url,
        "api_version": config.effective_api_version,
        "endpoint_source": config.endpoint_source,
        "trace_url": config.trace_url,
        "trace_key": config.trace_key,
        "workspace_root": workspace_root,
    }


def resolve_loop_type(decorator_loop: Optional[str], config: AgentConfiguration) -> str:
    """Loop type: config wins, then the decorator, then the built-in default."""
    return config.loop or decorator_loop or DEFAULT_AGENT_LOOP


__all__ = [
    "AgentLoop",
    "distinct_tools_used",
    "resolve_agent_loop",
    "resolve_agent_settings",
    "resolve_loop_type",
    "split_model_id",
]
