"""Tests for the agent loop abstraction, its plugin hook, and settings resolution."""

import asyncio
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, TypeVar, cast

import click
import pytest
from claude_agent_sdk import ProcessError
from pydantic_ai import ModelRetry, ToolFailed, capture_run_messages
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import (
    FunctionToolResultEvent,
    ModelResponse,
    PartEndEvent,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from dlt.common import json, logger
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.providers import EnvironProvider

from dlt._workspace.deployment.agent.exceptions import (
    AgentRunFailed,
    AgentTokenLimitExceeded,
    AgentTraceNotAvailable,
    UnknownAgentLoop,
    UnsupportedAgentModel,
)
from dlt._workspace.deployment.agent.loop import (
    AgentLoop,
    DEFAULT_USER_TURN,
    split_model_id,
    resolve_agent_loop,
    resolve_agent_settings,
    resolve_loop_type,
)
from dlt._workspace.deployment.agent.loops import claude_sdk
from dlt._workspace.deployment.agent.loops.claude_sdk import (
    AI_LOOP_TOOLS,
    MCP_TOOL_PATTERN,
    RULES_EXCLUDES,
    ClaudeAgentSdkLoop,
    classify_tool,
)
from dlt._workspace.deployment.agent.loops.pydantic_ai import PydanticAILoop, make_local_tools
from dlt._workspace.deployment.agent.loops.tools import (
    MCP_SERVER_ID,
    SHELL_TOOL,
    LocalTools,
    temp_dir,
)
from dlt._workspace.deployment.agent.manifest import load_agent_spec, resolve_agent_dir
from dlt._workspace.deployment.agent.typing import TAgentSpec
from dlt._workspace.deployment.configuration import AgentConfiguration
from dlt._workspace.deployment.launchers import (
    DEFAULT_AGENT_LOOP,
    LOOP_CLAUDE_AGENT_SDK,
    LOOP_PYDANTIC_AI,
)
from dlt._workspace.deployment.typing import TWorkspaceAccess

from tests.utils import init_test_logging, inject_providers
from tests.workspace.utils import importable_workspace

TLoop = TypeVar("TLoop", bound=AgentLoop)


@pytest.fixture
def dlt_logger_name() -> Iterator[str]:
    """Name of the initialized dlt logger, made to propagate so `caplog` sees it.

    Inside a workspace the logger is named after the workspace, not `dlt`.
    """
    init_test_logging()
    dlt_logger = logging.getLogger(logger.LOGGER.name)
    previous = dlt_logger.propagate
    dlt_logger.propagate = True
    try:
        yield dlt_logger.name
    finally:
        dlt_logger.propagate = previous


@pytest.fixture
def workspace() -> Iterator[Any]:
    with importable_workspace("agent_workspace", "mock_loop") as ctx:
        yield ctx


@pytest.fixture
def loop_cls(workspace: Any) -> Type[AgentLoop]:
    """The loop the workspace registers on import, as a plugin would."""
    import mock_loop  # type: ignore[import-not-found]

    return mock_loop.MockLoop


def _spec(run_dir: str) -> TAgentSpec:
    return load_agent_spec(resolve_agent_dir("dlthub-platform:job-inspector", run_dir))


def _bare_spec(**extra: Any) -> TAgentSpec:
    """An agent definition declaring nothing beyond what the contract requires."""
    spec: TAgentSpec = {
        "name": "bare",
        "description": "d",
        "access": {},
        "inputs": {"type": "object", "properties": {}, "prompt": ""},
        "output": {},
        "system_prompt": "b",
    }
    spec.update(cast(Any, extra))
    return spec


def _config(**overrides: Any) -> AgentConfiguration:
    config = AgentConfiguration()
    for key, value in overrides.items():
        setattr(config, key, value)
    return config


def _loop(
    workspace: Any,
    loop_cls: Type[TLoop],
    config: Optional[AgentConfiguration] = None,
    decorator_args: Optional[Dict[str, Any]] = None,
    **spec_overrides: Any,
) -> TLoop:
    """A loop initialized with the job-inspector definition, as the launcher builds one."""
    spec = _spec(workspace.run_dir)
    spec.update(cast(Any, spec_overrides))
    settings = resolve_agent_settings(
        spec, config or _config(), decorator_args or {}, loop_cls, workspace.run_dir
    )
    loop = loop_cls(settings)
    loop.init(spec)
    return loop


def _resolve_agent_config() -> AgentConfiguration:
    """The job's agent config from the environment alone, so a developer's own secrets stay out."""
    with inject_providers([EnvironProvider()]):
        return resolve_configuration(AgentConfiguration(), sections=("jobs", "ops", "inspector"))


def test_resolve_agent_loop_knows_builtins_and_plugins(loop_cls: Type[AgentLoop]) -> None:
    assert resolve_agent_loop(LOOP_PYDANTIC_AI).LOOP_TYPE == LOOP_PYDANTIC_AI
    assert resolve_agent_loop(LOOP_CLAUDE_AGENT_SDK).LOOP_TYPE == LOOP_CLAUDE_AGENT_SDK
    # a plugin answers only for the names it implements, so the builtins still resolve
    assert resolve_agent_loop(loop_cls.LOOP_TYPE) is loop_cls


def test_unknown_loop_names_the_builtins() -> None:
    with pytest.raises(UnknownAgentLoop, match=LOOP_PYDANTIC_AI):
        resolve_agent_loop("no-such-loop")


@pytest.mark.parametrize(
    "decorator_loop,config_loop,expected",
    [
        (None, None, DEFAULT_AGENT_LOOP),
        ("mock-loop", None, "mock-loop"),
        ("mock-loop", LOOP_CLAUDE_AGENT_SDK, LOOP_CLAUDE_AGENT_SDK),
    ],
    ids=["default", "decorator", "config-wins"],
)
def test_resolve_loop_type(
    decorator_loop: Optional[str], config_loop: Optional[str], expected: str
) -> None:
    assert resolve_loop_type(decorator_loop, _config(loop=config_loop)) == expected


def test_settings_precedence_rises_to_config(workspace: Any, loop_cls: Type[AgentLoop]) -> None:
    """loop default < AGENT.md defaults < decorator argument < resolved config."""
    # a definition declaring no defaults gets the loop's
    settings = resolve_agent_settings(_bare_spec(), _config(), {}, loop_cls, "/ws")
    assert settings["model"] == "mock-model"
    assert (settings["max_turns"], settings["max_tokens"]) == (5, 1000)

    # nothing supplied: the spec's own defaults beat the loop class defaults
    spec = _spec(workspace.run_dir)
    settings = resolve_agent_settings(spec, _config(), {}, loop_cls, workspace.run_dir)
    assert settings["model"] == "sonnet"
    assert settings["max_turns"] == 30
    assert settings["max_tokens"] == 1000000
    assert settings["verbosity"] == 1

    # a decorator argument beats the spec
    decorator_args = {"model": "opus", "verbosity": 0}
    settings = resolve_agent_settings(spec, _config(), decorator_args, loop_cls, workspace.run_dir)
    assert settings["model"] == "opus"
    assert settings["verbosity"] == 0

    # config beats the decorator
    config = _config(model="haiku", verbosity=2)
    settings = resolve_agent_settings(spec, config, decorator_args, loop_cls, workspace.run_dir)
    assert settings["model"] == "haiku"
    assert settings["verbosity"] == 2

    # one limit overridden leaves the other where the spec put it
    settings = resolve_agent_settings(spec, _config(max_turns=3), {}, loop_cls, workspace.run_dir)
    assert settings["max_turns"] == 3
    assert settings["max_tokens"] == 1000000

    # loop run args merge shallowly: the spec default `retries: 1` goes, everything else survives
    settings = resolve_agent_settings(
        spec,
        _config(loop_run_args={"tool_timeout": 30}),
        {"loop_run_args": {"retries": 5, "extra": 1}},
        loop_cls,
        workspace.run_dir,
    )
    assert settings["loop_run_args"] == {"retries": 5, "extra": 1, "tool_timeout": 30}


def test_instructions_resolve_like_the_model(workspace: Any, loop_cls: Type[AgentLoop]) -> None:
    """The user turn comes from the decorator, then configuration, then the run itself."""
    spec = _spec(workspace.run_dir)

    # nobody said anything: the system prompt speaks alone and the run still opens
    settings = resolve_agent_settings(spec, _config(), {}, loop_cls, workspace.run_dir)
    assert settings["instructions"] is None
    assert loop_cls(settings).user_turn == DEFAULT_USER_TURN

    settings = resolve_agent_settings(
        spec,
        _config(instructions="focus on the loader step"),
        {"instructions": "explain the failure"},
        loop_cls,
        workspace.run_dir,
    )
    assert loop_cls(settings).user_turn == "focus on the loader step"

    # and a direct call overrides both
    loop = loop_cls(settings)
    loop.resolve_run(instructions="just list the failed jobs")
    assert loop.user_turn == "just list the failed jobs"


@pytest.mark.parametrize(
    "model,provider,expected",
    [
        ("sonnet", "anthropic", "anthropic:claude-sonnet-5"),
        ("gpt", "anthropic", "openai:gpt-5.5"),
        ("claude-opus-5", "anthropic", "anthropic:claude-opus-5"),
        ("openai:gpt-5.4-mini", "anthropic", "openai:gpt-5.4-mini"),
        ("gateway/openai:gpt-5.5", "anthropic", "gateway/openai:gpt-5.5"),
        ("mock-model", "", "mock-model"),
    ],
    ids=["alias", "other-provider-alias", "bare", "qualified", "gateway", "no-provider"],
)
def test_model_id_is_provider_qualified(
    workspace: Any, loop_cls: Type[AgentLoop], model: str, provider: str, expected: str
) -> None:
    """dlt names models as pydantic-ai does; a loop qualifies a bare name with its own provider."""
    spec = _spec(workspace.run_dir)
    settings = resolve_agent_settings(spec, _config(model=model), {}, loop_cls, workspace.run_dir)
    loop = type("Loop", (loop_cls,), {"DEFAULT_PROVIDER": provider})(settings)

    assert loop.model_id() == expected


@pytest.mark.parametrize(
    "model,expected",
    [
        ("anthropic:claude-sonnet-5", ("anthropic", "claude-sonnet-5")),
        ("gateway/openai:gpt-5.5", ("gateway/openai", "gpt-5.5")),
        ("mock-model", ("", "mock-model")),
    ],
    ids=["qualified", "gateway", "bare"],
)
def test_split_model_id(model: str, expected: Tuple[str, str]) -> None:
    assert split_model_id(model) == expected


@pytest.mark.parametrize("user_field", ["MODEL", "API_KEY", "API_URL", "API_VERSION"])
def test_the_endpoint_is_one_set_and_any_user_field_replaces_all_of_it(user_field: str) -> None:
    """A model id belongs to the endpoint that serves it, so a run never mixes the two sets."""
    # the runtime set sits on the workspace-wide section, which every job falls back to
    os.environ["AGENT__RUNTIME_MODEL"] = "runtime-model"
    os.environ["AGENT__RUNTIME_API_KEY"] = "runtime-key"
    os.environ["AGENT__RUNTIME_API_URL"] = "https://runtime.example"
    os.environ["AGENT__RUNTIME_API_VERSION"] = "2024-02-01"
    config = _resolve_agent_config()

    assert config.endpoint_source == "runtime"
    assert (
        config.effective_model,
        config.effective_api_key,
        config.effective_api_url,
        config.effective_api_version,
    ) == ("runtime-model", "runtime-key", "https://runtime.example", "2024-02-01")

    # one user field on the job's own section takes the whole set with it, the runtime model included
    os.environ[f"JOBS__OPS__INSPECTOR__AGENT__{user_field}"] = "user-value"
    config = _resolve_agent_config()

    assert config.endpoint_source == "user"
    effective = {
        "MODEL": config.effective_model,
        "API_KEY": config.effective_api_key,
        "API_URL": config.effective_api_url,
        "API_VERSION": config.effective_api_version,
    }
    assert effective.pop(user_field) == "user-value"
    assert set(effective.values()) == {None}
    # the runtime values stay readable rather than being overwritten
    assert config.runtime_model == "runtime-model"
    assert config.runtime_api_key == "runtime-key"


def test_a_user_key_leaves_the_runtime_model_out_of_the_run(loop_cls: Type[AgentLoop]) -> None:
    """The user's endpoint does not serve the runtime's model id, so the agent's own model runs."""
    spec = _bare_spec(defaults={"model": "declared-model"})
    runtime = _config(
        runtime_model="runtime-model",
        runtime_api_key="runtime-key",
        runtime_api_version="2024-02-01",
    )
    settings = resolve_agent_settings(spec, runtime, {}, loop_cls, "/ws")
    assert (settings["model"], settings["api_key"]) == ("runtime-model", "runtime-key")
    assert settings["api_version"] == "2024-02-01"

    with_user_key = _config(
        runtime_model="runtime-model",
        runtime_api_key="runtime-key",
        runtime_api_version="2024-02-01",
        api_key="user-key",
        api_version="2024-10-21",
    )
    settings = resolve_agent_settings(spec, with_user_key, {}, loop_cls, "/ws")
    assert (settings["model"], settings["api_key"]) == ("declared-model", "user-key")
    # the version follows the key, and both values stay readable side by side
    assert settings["api_version"] == "2024-10-21"
    assert with_user_key.runtime_api_version == "2024-02-01"


def test_a_loop_run_from_settings_to_trace(
    workspace: Any,
    loop_cls: Type[AgentLoop],
    caplog: pytest.LogCaptureFixture,
    dlt_logger_name: str,
) -> None:
    """One loop through its life: built from settings, given the definition, run, and run again."""
    spec = _spec(workspace.run_dir)
    settings = resolve_agent_settings(
        spec, _config(api_key="user-key"), {}, loop_cls, workspace.run_dir
    )
    loop = loop_cls(settings)
    loop.agent_ref = "dlthub-platform:job-inspector"
    loop.agent_file = ".claude/dlthub/agents/job-inspector/AGENT.md"
    # nothing is recorded and nothing is built until a run supplies its arguments
    with pytest.raises(AgentTraceNotAvailable):
        loop.trace
    loop.init(spec)
    assert loop.native is None

    with caplog.at_level(logging.INFO, logger=dlt_logger_name):
        loop.resolve_run()
    assert any("on the user endpoint" in r.getMessage() for r in caplog.records)

    asyncio.run(loop.run(inputs={"failed_job_ref": "jobs.batch.ingest"}))
    assert loop.native == "mock-native:anthropic:claude-sonnet-5"
    trace = loop.trace
    assert trace["agent"] == "dlthub-platform:job-inspector"
    assert trace["agent_file"] == ".claude/dlthub/agents/job-inspector/AGENT.md"
    assert trace["loop_type"] == loop_cls.LOOP_TYPE
    assert trace["model"] == "anthropic:claude-sonnet-5"
    assert trace["limits"] == {"max_turns": 30, "max_tokens": 1000000}
    assert trace["loop_run_args"] == {"retries": 1}
    assert trace["inputs"] == {"failed_job_ref": "jobs.batch.ingest"}
    assert trace["mcp_features"] == ["telemetry"]
    # the mock wires no local tool, whatever the declaration asks for
    assert trace["local_tools"] == {}

    # `run` takes the runtime and rebuilds the native object; one limit overridden leaves the
    # other where the declaration put it
    asyncio.run(
        loop.run(inputs={}, model="haiku", limits={"max_turns": 3}, instructions="be brief")
    )
    assert loop.native == "mock-native:anthropic:claude-haiku-4-5"
    assert loop.user_turn == "be brief"
    assert loop.trace["instructions"] == "be brief"
    assert loop.trace["limits"] == {"max_turns": 3, "max_tokens": 1000000}

    # the beacon swallows serialization errors, so one exotic input must not drop the run
    asyncio.run(loop.run(inputs={"handle": object(), "nested": {"fn": lambda: None}}))
    json.dumps(loop.trace)
    assert loop.trace["inputs"]["handle"].startswith("<object object")

    # finishing names the tools the run used, in the log and in the console event
    events: List[Dict[str, Any]] = []
    native: Any = loop
    native.emit = lambda kind, **fields: events.append({"kind": kind, **fields})
    with caplog.at_level(logging.INFO, logger=dlt_logger_name):
        loop.emit_run_finished("succeeded")
        loop.trace["tools_used"] = ["read_file"]
        loop.trace["mcp_tools_used"] = ["list_runs", "get_logs"]
        loop.emit_run_finished("succeeded")
    messages = [r.getMessage() for r in caplog.records]
    assert any("finished in 3 turns and 165 tokens, using no tools" in m for m in messages)
    assert any("using tools: read_file; mcp tools: list_runs, get_logs" in m for m in messages)
    assert events[-1]["tools"] == ["read_file"]
    assert events[-1]["mcp_tools"] == ["list_runs", "get_logs"]

    # the base counts every turn, so `max_tokens` means the same on any framework: the mock
    # counts three turns of 55 tokens, and the second one passes a limit of 100
    with pytest.raises(AgentTokenLimitExceeded, match="used 110 tokens, over its limit of 100"):
        asyncio.run(
            loop.run(inputs={"failed_job_ref": "jobs.b.ingest"}, limits={"max_tokens": 100})
        )
    assert loop.tokens_used == 110


def test_loops_wire_only_what_the_agent_declares(workspace: Any) -> None:
    """No `access`: no file tool, no shell, and a server told in so many words to grant nothing.
    No `tools`: no server at all."""

    for loop_cls in (PydanticAILoop, ClaudeAgentSdkLoop):
        loop = _loop(workspace, loop_cls, access={})
        assert loop.spec["tools"]
        assert loop.local_tools() == {}, loop_cls.LOOP_TYPE
        if isinstance(loop, PydanticAILoop):
            args = loop._build_toolsets()[0].client.transport.args
        else:
            options = cast(ClaudeAgentSdkLoop, loop)._build_options("system")
            args = options.mcp_servers[MCP_SERVER_ID]["args"]
        # the server still serves the tools that require nothing, such as the toolkit catalogue
        assert args[args.index("--access") + 1] == "local:,data:,context:", loop_cls.LOOP_TYPE

        # `tools` is the whole request: nothing declared, nothing spawned
        loop = _loop(workspace, loop_cls, tools=[], access={"local": ["read"]})
        if isinstance(loop, PydanticAILoop):
            assert loop._build_toolsets() == []
        else:
            options = cast(ClaudeAgentSdkLoop, loop)._build_options("system")
            assert options.mcp_servers == {}
            assert MCP_TOOL_PATTERN not in options.allowed_tools


def test_loops_serve_the_workspace_mcp_server(workspace: Any) -> None:
    """Each loop spawns the workspace server itself, for the feature groups the agent declared."""

    for loop_cls in (PydanticAILoop, ClaudeAgentSdkLoop):
        loop = _loop(workspace, loop_cls, access={"local": ["read"]}, tools=["telemetry"])
        if isinstance(loop, PydanticAILoop):
            transport = loop._build_toolsets()[0].client.transport
            args, env = transport.args, transport.env
            assert transport.cwd == workspace.run_dir
        else:
            options = cast(ClaudeAgentSdkLoop, loop)._build_options("system")
            server = options.mcp_servers[MCP_SERVER_ID]
            args, env = server["args"], server["env"]
        assert args[:4] == ["ai", "mcp", "run", "--stdio"], loop_cls.LOOP_TYPE
        # the agent gets the feature group it declared and none of the interactive defaults
        assert "--no-default-features" in args, loop_cls.LOOP_TYPE
        assert args[-2:] == ["--features", "telemetry"], loop_cls.LOOP_TYPE
        assert "secrets" not in args, loop_cls.LOOP_TYPE
        assert env["FASTMCP_SHOW_SERVER_BANNER"] == "false", loop_cls.LOOP_TYPE
        # the server reads the profile the launcher exported
        assert "WORKSPACE__PROFILE" in env or os.environ.get("WORKSPACE__PROFILE") is None

    # project settings are read for the skills, never for servers: the project's own `.mcp.json`
    # stays out, and the workspace server's tools ride along on `allowed_tools`
    assert options.setting_sources == ["project"]
    assert options.strict_mcp_config is True
    assert MCP_TOOL_PATTERN in options.allowed_tools


def test_loops_hand_entity_types_to_the_model_as_comments(workspace: Any) -> None:
    """The Claude CLI validates the output schema strictly, so dlt's keyword travels as `$comment`."""

    output = _spec(workspace.run_dir)["output"]
    output["properties"]["classification"]["entity_type"] = "job"
    for loop_cls in (PydanticAILoop, ClaudeAgentSdkLoop):
        loop = _loop(workspace, loop_cls, config=_config(api_key="sk-test"), output=output)
        if isinstance(loop, PydanticAILoop):
            agent_spec = loop._agent_spec_dict(loop._build_model())
            schemas = [agent_spec["deps_schema"], agent_spec["output_schema"]]
            assert schemas[0]["properties"]["failed_run_id"]["$comment"] == "entity_type: job-run"
        else:
            options = cast(ClaudeAgentSdkLoop, loop)._build_options("system")
            schemas = [options.output_format["schema"]]
        assert '"entity_type"' not in json.dumps(schemas), loop_cls.LOOP_TYPE
        assert schemas[-1]["properties"]["classification"]["$comment"] == "entity_type: job"
        # the manifest schema keeps the keyword
        assert loop.spec["inputs"]["properties"]["failed_run_id"]["entity_type"] == "job-run"


def test_both_loops_render_the_body_and_keep_the_turn_out_of_it(workspace: Any) -> None:
    """The system prompt is the rendered body plus what the loop inlines; the turn stays a turn."""

    inputs = {"failed_run_id": "r-77", "run_context": {"trigger": "job.fail:*"}}
    for loop_cls in (PydanticAILoop, ClaudeAgentSdkLoop):
        loop = _loop(workspace, loop_cls, config=_config(instructions="focus on the loader step"))
        rendered = loop.render_system_prompt(inputs)

        assert "with failed run id 'r-77'" in rendered, loop_cls.LOOP_TYPE
        # the rule the agent declared is inlined next to the body
        assert "Resource changes are proposals" in rendered, loop_cls.LOOP_TYPE
        assert "focus on the loader step" not in rendered, loop_cls.LOOP_TYPE
        assert loop.user_turn == "focus on the loader step", loop_cls.LOOP_TYPE
        assert loop._unresolved_placeholders == ["failed_job_ref"], loop_cls.LOOP_TYPE
    # the claude loop hands the framework what it rendered; the pydantic one has its own test
    assert cast(Any, loop)._build_options(rendered).system_prompt == rendered


def test_loops_name_the_workspace_and_the_temp_folder(workspace: Any) -> None:
    """The tools' descriptions stay generic, so the prompt says which folders they mean."""

    for loop_cls in (PydanticAILoop, ClaudeAgentSdkLoop):
        loop = _loop(workspace, loop_cls)
        prompt = loop.render_system_prompt({})
        # both folders as posix paths, whatever the platform
        assert Path(workspace.run_dir).resolve().as_posix() in prompt, loop_cls.LOOP_TYPE
        assert temp_dir().as_posix() in prompt, loop_cls.LOOP_TYPE
    # the CLI's file tools stop at the working directory unless the temp folder is added
    assert cast(Any, loop)._build_options("system").add_dirs == [str(temp_dir())]


def test_pydantic_loop_stops_at_the_token_limit(workspace: Any) -> None:
    """A real run: the check sits in the event handler, and its raise ends the agent run."""

    # local read tools give TestModel something to call, so the run takes more than one turn
    loop = _loop(workspace, PydanticAILoop, access={"local": ["read"]})
    native: Any = loop
    native._build_model = lambda: TestModel()
    native._build_toolsets = lambda: []

    with pytest.raises(AgentTokenLimitExceeded, match="over its limit of 1"):
        asyncio.run(
            loop.run(inputs={"failed_run_id": "r-1", "run_context": {}}, limits={"max_tokens": 1})
        )
    assert loop.tokens_used > 1


class _Usage:
    requests = 2
    input_tokens = 10
    output_tokens = 5
    total_tokens = 15


@pytest.mark.parametrize("usage_is_callable", [False, True], ids=["property", "method"])
def test_pydantic_loop_reads_usage_either_way(workspace: Any, usage_is_callable: bool) -> None:
    """`usage` is a property from pydantic-ai 2.36 on; calling it as a method killed the run."""

    class _Result:
        usage = (lambda self: _Usage()) if usage_is_callable else _Usage()

        def all_messages(self) -> List[Any]:
            return []

    trace = _loop(workspace, PydanticAILoop)._build_trace({}, _Result())

    assert trace["turn_count"] == 2
    assert (trace["input_tokens"], trace["output_tokens"], trace["total_tokens"]) == (10, 5, 15)


def test_pydantic_trace_tells_mcp_tools_from_local_ones(workspace: Any) -> None:
    """A tool the loop did not build itself came from the workspace MCP server. The run's tool
    lists name each tool once, in the order it was first used."""

    class _Result:
        usage = _Usage()

        def all_messages(self) -> List[Any]:
            return [
                ModelResponse(
                    parts=[
                        ToolCallPart(tool_name="Read", args={"path": "x"}),
                        ToolCallPart(tool_name="list_toolkits", args={}),
                    ]
                ),
                ModelResponse(
                    parts=[
                        ToolCallPart(tool_name="Grep", args={"pattern": "x"}),
                        ToolCallPart(tool_name="Read", args={"path": "y"}),
                        ToolCallPart(tool_name="toolkit_info", args={"name": "t"}),
                        ToolCallPart(tool_name="list_toolkits", args={}),
                        # the answer is not a tool the agent used
                        ToolCallPart(tool_name="final_result", args={}),
                    ]
                ),
            ]

    loop = _loop(workspace, PydanticAILoop, access=TWorkspaceAccess(local=["read"]))
    loop._build_tools()
    trace = loop._build_trace({}, _Result())

    assert trace["turns"][0]["tools"] == [
        {"name": "Read", "kind": "builtin"},
        {"name": "list_toolkits", "kind": "mcp", "server": "dlt-workspace-mcp"},
    ]
    assert [use["name"] for use in trace["turns"][1]["tools"]] == [
        "Grep",
        "Read",
        "toolkit_info",
        "list_toolkits",
    ]
    assert trace["tools_used"] == ["Read", "Grep"]
    assert trace["mcp_tools_used"] == ["list_toolkits", "toolkit_info"]


def test_pydantic_loop_reports_what_the_agent_does(
    workspace: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    loop = _loop(workspace, PydanticAILoop)
    # the handler reads the run's usage on entry; nothing was used yet
    ctx = SimpleNamespace(usage=RunUsage())

    async def _drive() -> None:
        async def events() -> Any:
            yield PartEndEvent(index=0, part=ThinkingPart(content="weighing the options"))
            yield PartEndEvent(index=1, part=TextPart(content="here is the answer"))
            yield PartEndEvent(index=2, part=ToolCallPart(tool_name="list_runs", args={"n": 3}))
            yield FunctionToolResultEvent(
                part=ToolReturnPart(tool_name="list_runs", content="r-1 failed", tool_call_id="1")
            )
            # the answer arrives as a call to pydantic-ai's output tool
            yield PartEndEvent(
                index=3,
                part=ToolCallPart(
                    tool_name="final_result", args={"summary": "r-1 ran out of memory"}
                ),
            )
            yield FunctionToolResultEvent(
                part=ToolReturnPart(
                    tool_name="final_result", content="Final result processed.", tool_call_id="2"
                )
            )

        await loop._emit_events(ctx, events())

    loop.emit_run_start("inspect the failed run")
    asyncio.run(_drive())

    # the transcript goes to stdout, terminal or not; colors are stripped to match the text
    out = click.unstyle(capsys.readouterr().out)
    assert "job-inspector" in out and "anthropic:claude-sonnet-5" in out
    assert "prompt\n  inspect the failed run" in out
    assert "thinks  weighing the options" in out
    assert "says\n  here is the answer" in out
    assert 'list_runs (dlt-workspace-mcp)  {"n":3}' in out
    # the result lives on the part; reporting `event.content` showed nothing
    assert "→ r-1 failed" in out
    # the answer is the agent speaking, not a tool call to an MCP server it never made
    assert "says\n  r-1 ran out of memory" in out
    assert "final_result" not in out


def test_pydantic_loop_reports_a_tool_it_could_not_get_past(workspace: Any) -> None:
    """A tool that keeps failing ends the run: say which one, and where the limit lives."""

    loop = _loop(workspace, PydanticAILoop)

    class _Failing:
        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *args: Any) -> None:
            return None

        async def run(self, *args: Any, **kwargs: Any) -> Any:
            raise UnexpectedModelBehavior("Tool 'list_recent_runs' exceeded max retries count of 1")

    loop._build_agent = lambda *args: _Failing()  # type: ignore[method-assign]

    with pytest.raises(AgentRunFailed) as failed:
        asyncio.run(loop.run(inputs={}))

    said = str(failed.value)
    assert "list_recent_runs" in said
    assert "loop_run_args.retries" in said


def test_system_prompt_is_shown_in_full_at_the_top_verbosity(
    workspace: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """The prompt is what the agent actually got: too long for every run, too useful to drop."""
    loop = _loop(workspace, PydanticAILoop)
    loop._system_prompt = "RULE " * 400

    loop.settings["verbosity"] = 1
    loop.render_system_prompt({})
    assert "system prompt" not in click.unstyle(capsys.readouterr().out)

    loop.settings["verbosity"] = 2
    loop.render_system_prompt({})
    out = click.unstyle(capsys.readouterr().out)
    assert "system prompt\n  RULE RULE" in out
    assert out.count("RULE") == 400


def test_pydantic_loop_hands_the_prompt_over_verbatim(workspace: Any) -> None:
    """pydantic-ai renders `AgentSpec.instructions` as a template when `deps_schema` is set.

    dlt has already rendered the prompt, so a `{{ }}` left in a rule, a skill or an example
    would be silently dropped on the way to the model.
    """

    # dlt blanks its own `{{ name }}`; a call is not its grammar, and must survive untouched
    body = _spec(workspace.run_dir)["system_prompt"]
    body += "\n\nAn example the agent must still see: {{helper(x)}}."
    # native capabilities are anthropic's, and TestModel refuses them
    loop = _loop(workspace, PydanticAILoop, system_prompt=body, access={"data": ["read"]})
    rendered = loop.render_system_prompt({"failed_run_id": "r-77", "run_context": {}})

    native: Any = loop
    native._build_model = lambda: TestModel()
    native._build_toolsets = lambda: []
    agent = native._build_agent(rendered)
    with capture_run_messages() as messages:
        asyncio.run(agent.run("go", deps={"failed_run_id": "r-77"}))

    assert "{{helper(x)}}" in rendered
    assert cast(Any, messages[0]).instructions == rendered


@pytest.mark.parametrize("retries", [0, 2], ids=["no-budget", "budget"])
def test_tool_errors_follow_the_retry_budget(workspace: Any, tmp_path: Path, retries: int) -> None:
    """0 hands a tool error to the model as a failed call; above it pydantic-ai asks for a fix."""

    # the spec's own default is 1, the decorator argument is what the run gets
    loop = _loop(
        workspace,
        PydanticAILoop,
        config=_config(api_key="sk-test"),
        decorator_args={"loop_run_args": {"retries": retries}},
    )

    assert loop.tool_retries == retries
    assert loop._build_toolsets()[0].tool_error_behavior == ("retry" if retries else "failed")
    # the agent's own budget is forwarded only when there is one to enforce
    assert loop._agent_spec_dict(loop._build_model()).get("retries") == (retries or None)
    # the same tool error reaches pydantic-ai as a retry or as a failed call
    tools = {t.name: t for t in make_local_tools(LocalTools(str(tmp_path)), {"read"}, retries)}
    with pytest.raises(ToolFailed if retries == 0 else ModelRetry, match="does not exist"):
        tools["Read"].function("missing.txt")


def test_pydantic_loop_hands_a_failed_tool_call_to_the_model(workspace: Any) -> None:
    """A real run without a budget: the missing file comes back as a result, and the run goes on."""

    loop = _loop(
        workspace,
        PydanticAILoop,
        decorator_args={"loop_run_args": {"retries": 0}},
        access={"local": ["read"]},
    )
    native: Any = loop
    native._build_model = lambda: TestModel()
    native._build_toolsets = lambda: []
    events: List[Dict[str, Any]] = []
    emit = loop.emit

    def recording(kind: Any, **fields: Any) -> None:
        events.append({"kind": kind, **fields})
        emit(kind, **fields)

    native.emit = recording

    # TestModel calls Read with a made-up path, which is the failure the model must get to see
    output = asyncio.run(loop.run(inputs={"failed_run_id": "r-1", "run_context": {}}))

    assert isinstance(output, dict)
    failed = [e for e in events if e["kind"] == "tool_result" and e.get("error")]
    assert failed and "does not exist" in str(failed[0]["detail"])
    assert events[-1]["kind"] == "finish"


@pytest.mark.parametrize(
    "local_access,expected",
    [
        (["read"], ["Read", "Glob", "Grep"]),
        (["read", "write"], ["Read", "Glob", "Grep", "Write", "Edit"]),
        (["execute"], [SHELL_TOOL, "RunPython"]),
        (["all"], ["Read", "Glob", "Grep", "Write", "Edit", SHELL_TOOL, "RunPython"]),
        ([], []),
        # the provider runs the network tools itself, so no function is registered
        (["network"], []),
    ],
    ids=["read", "read-write", "execute", "all", "none", "network"],
)
def test_pydantic_loop_local_tools_follow_access(
    workspace: Any, local_access: List[str], expected: List[str]
) -> None:
    """Both loops answer a verb with the same tool names, whatever implements them."""
    loop = _loop(workspace, PydanticAILoop, access=TWorkspaceAccess(local=cast(Any, local_access)))
    assert [tool.name for tool in loop._build_tools()] == expected


@pytest.mark.parametrize(
    "model,expected_system",
    [("sonnet", "anthropic"), ("openai:gpt-5.5", "openai"), ("gemini", "google")],
    ids=["anthropic", "openai", "google"],
)
def test_pydantic_loop_builds_any_provider(
    workspace: Any, model: str, expected_system: str
) -> None:
    """One key and url reach whichever provider the model names."""

    loop = _loop(workspace, PydanticAILoop, config=_config(model=model, api_key="sk-test"))
    built = loop._build_model()

    assert built.system == expected_system
    assert built.model_name == split_model_id(loop.model_id())[1]


@pytest.mark.parametrize(
    "provider",
    ["openai", "azure", "litellm", "anthropic", "google"],
    ids=["base_url", "azure_endpoint", "api_base", "anthropic", "google"],
)
def test_api_url_reaches_the_provider_under_its_own_argument(workspace: Any, provider: str) -> None:
    """Every provider names the endpoint its own way, and dropping it talks to the wrong host."""

    loop = _loop(workspace, PydanticAILoop)
    loop.settings["api_key"] = "sk-test"
    loop.settings["api_url"] = "https://gateway.example.com/v1"

    assert (
        str(loop._build_provider(provider).base_url).rstrip("/") == "https://gateway.example.com/v1"
    )


def test_a_provider_takes_its_own_endpoint_arguments(workspace: Any) -> None:
    """Azure versions its data plane; deepseek runs on its own platform; nobody knows `nope`."""

    loop = _loop(workspace, PydanticAILoop)
    loop.settings["api_key"] = "sk-test"
    loop.settings["api_url"] = "https://my-resource.openai.azure.com"
    loop.settings["api_version"] = "2024-10-21"
    assert loop._build_provider("azure").client._custom_query == {"api-version": "2024-10-21"}

    # silently ignoring `api_url` sent the run to the provider's own platform
    with pytest.raises(UnsupportedAgentModel, match="takes no api_url"):
        loop._build_provider("deepseek")

    loop = _loop(workspace, PydanticAILoop, config=_config(model="nope:x"))
    with pytest.raises(UnsupportedAgentModel, match="nope:x"):
        loop._build_model()


def test_pydantic_loop_serves_network_from_the_provider(workspace: Any) -> None:
    """Pydantic AI ships no web tool of its own; the provider runs both web tools."""
    loop = _loop(workspace, PydanticAILoop)
    loop.settings["loop_run_args"] = {"capabilities": [{"Thinking": {"effort": "medium"}}]}
    loop.settings["api_key"] = "sk-test"
    model = loop._build_model()

    loop.spec["access"] = TWorkspaceAccess(local=["read"])
    assert loop._agent_spec_dict(model)["capabilities"] == [{"Thinking": {"effort": "medium"}}]

    loop.spec["access"] = TWorkspaceAccess(local=["read", "network", "execute"])
    # what the manifest asked for survives; the web tools are added to it, `execute` adds nothing
    assert loop._agent_spec_dict(model)["capabilities"] == [
        {"Thinking": {"effort": "medium"}},
        {"WebSearch": {}},
        {"WebFetch": {}},
    ]
    # the spec must pass pydantic-ai's capability registry; pydantic-ai adds native tools of
    # its own (tool search), so the two are a subset
    agent = loop._build_agent("prompt")
    assert {"WebSearchTool", "WebFetchTool"} <= {type(t).__name__ for t in agent._cap_native_tools}
    # the function tools and the provider's own, each under its verb
    loop._build_tools()
    assert loop.local_tools() == {
        "Read": "read",
        "Glob": "read",
        "Grep": "read",
        SHELL_TOOL: "execute",
        "RunPython": "execute",
        "WebFetch": "network",
        "WebSearch": "network",
    }


def test_provider_capabilities_are_dropped_when_the_model_cannot_serve_them(
    workspace: Any,
) -> None:
    """OpenAI responses search the web, but fetch no URL of their own."""

    loop = _loop(
        workspace,
        PydanticAILoop,
        config=_config(model="openai:gpt-5.5", api_key="sk-test"),
        access=TWorkspaceAccess(local=["network", "execute"]),
    )

    assert loop._agent_spec_dict(loop._build_model())["capabilities"] == [{"WebSearch": {}}]


@pytest.mark.parametrize(
    "name,tool_input,expected",
    [
        ("Bash", None, {"name": "Bash", "kind": "builtin"}),
        (
            "mcp__dlt-workspace-mcp__list_tables",
            None,
            {"name": "list_tables", "kind": "mcp", "server": "dlt-workspace-mcp"},
        ),
        # a server name is not guaranteed to be there
        ("mcp__bare", None, {"name": "bare", "kind": "mcp"}),
        ("Skill", {"skill": "debug-deployment"}, {"name": "debug-deployment", "kind": "skill"}),
        # the harness may name the skill under another key, or not at all
        ("Skill", {"name": "profiling"}, {"name": "profiling", "kind": "skill"}),
        ("Skill", {}, {"name": "Skill", "kind": "skill"}),
    ],
    ids=["builtin", "mcp", "mcp-no-server", "skill", "skill-alt-key", "skill-unnamed"],
)
def test_classify_tool(name: str, tool_input: Any, expected: Dict[str, Any]) -> None:
    assert classify_tool(name, tool_input) == expected


@pytest.mark.parametrize(
    "local_access,expected",
    [
        (["read"], ["Read", "NotebookRead", "Glob", "Grep"]),
        (["write"], ["Write", "Edit", "MultiEdit", "NotebookEdit"]),
        # one shell per platform, as Claude Code offers it: PowerShell on Windows, bash elsewhere
        (["execute"], list(AI_LOOP_TOOLS[SHELL_TOOL])),
        (["network"], ["WebFetch", "WebSearch"]),
        ([], []),
    ],
    ids=["read", "write", "execute", "network", "none"],
)
def test_claude_loop_tools_follow_access(
    workspace: Any, local_access: List[str], expected: List[str]
) -> None:
    """`tools` is the availability list: no verb, no built-in, not even a denied one."""

    # the Skill tool follows `skills`, not access
    loop = _loop(
        workspace,
        ClaudeAgentSdkLoop,
        access=TWorkspaceAccess(local=cast(Any, local_access)),
        skills=[],
    )
    options = loop._build_options("system")

    assert options.tools == expected
    # what exists may still need approving; the MCP pattern rides along on `allowed_tools`
    assert options.allowed_tools[: len(expected)] == expected
    # each allowed CLI tool is listed under the verb that bought it, extensions included
    assert loop.local_tools() == {tool: verb for verb in local_access for tool in expected}


def test_claude_loop_lists_only_the_declared_skills(workspace: Any) -> None:
    """The installed skill is the CLI's to list and load; an agent declaring none must not see
    the skills of whoever installed the harness."""

    loop = _loop(workspace, ClaudeAgentSdkLoop)
    options = loop._build_options("system")
    assert options.skills == ["debug-deployment"]
    # the CLI lists skills only where the Skill tool exists, and `tools` is the base set
    assert "Skill" in options.tools
    # nothing of the skill is pasted into the prompt
    assert loop._inlined_skills == []
    assert "<skill" not in loop.render_system_prompt({})

    loop = _loop(workspace, ClaudeAgentSdkLoop, skills=[])
    options = loop._build_options("system")
    # `None` would leave the harness defaults in place, which list every skill it can find
    assert options.skills == []
    assert "Skill" not in options.tools
    assert "Skill" not in options.allowed_tools


def test_claude_loop_keeps_the_project_rules_out(workspace: Any, tmp_path: Path) -> None:
    """The project settings bring every rule file, which name all skills of the workspace.
    The agent declares its rules, so the CLI must not load any on its own. CLAUDE.md stays."""

    loop = _loop(workspace, ClaudeAgentSdkLoop)
    assert json.loads(loop._build_options("system").settings) == {
        "claudeMdExcludes": RULES_EXCLUDES
    }
    assert not any("CLAUDE" in glob for glob in RULES_EXCLUDES)
    # settings given in run args are kept, inline or from a file, and the exclusion stays
    given = {"claudeMdExcludes": ["**/TEAM.md"], "env": {"X": "1"}}
    loop.settings["loop_run_args"]["settings"] = json.dumps(given)
    merged = json.loads(loop._build_options("system").settings)
    assert merged["env"] == {"X": "1"}
    assert merged["claudeMdExcludes"] == [*RULES_EXCLUDES, "**/TEAM.md"]
    settings_file = tmp_path / "settings.json"
    settings_file.write_text(json.dumps(given), encoding="utf-8")
    loop.settings["loop_run_args"]["settings"] = str(settings_file)
    assert json.loads(loop._build_options("system").settings) == merged


def test_claude_loop_denies_its_file_tools_the_credentials(workspace: Any) -> None:
    """The harness owns its file tools, so the only lever is a deny rule per tool."""

    loop = _loop(workspace, ClaudeAgentSdkLoop, access={"local": ["read", "write"]})
    denied = loop._build_options("system").disallowed_tools

    assert "Read(**/*secrets.toml)" in denied
    assert "Write(**/*secrets.toml)" in denied
    assert "Grep(**/.env)" in denied


def test_claude_loop_takes_the_bare_anthropic_name(workspace: Any) -> None:
    """The harness names Anthropic models without the provider prefix, and runs nothing else."""

    loop = _loop(workspace, ClaudeAgentSdkLoop, config=_config(model="opus"))
    assert loop._ai_loop_model() == "claude-opus-5"
    assert loop._build_options("system").model == "claude-opus-5"

    loop = _loop(workspace, ClaudeAgentSdkLoop, config=_config(model="claude-haiku-4-5"))
    assert loop._ai_loop_model() == "claude-haiku-4-5"
    assert loop._build_options("system").model == "claude-haiku-4-5"

    loop = _loop(workspace, ClaudeAgentSdkLoop, config=_config(model="gpt"))
    with pytest.raises(UnsupportedAgentModel, match="pydantic-ai"):
        loop._ai_loop_model()


def test_claude_loop_keeps_the_cli_stderr_for_the_failure(
    workspace: Any,
    caplog: pytest.LogCaptureFixture,
    dlt_logger_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI writes notices to stderr, which the transcript must not show. When the CLI dies
    the SDK says only "check stderr", so the run failure carries what the CLI wrote there."""

    loop = _loop(workspace, ClaudeAgentSdkLoop)
    options = loop._build_options("system")
    with caplog.at_level(logging.DEBUG, logger=dlt_logger_name):
        options.stderr("claude.ai connectors are disabled because ANTHROPIC_API_KEY is set")

    assert any("connectors are disabled" in r.getMessage() for r in caplog.records)
    # kept for a run that ends without a result, where the harness never says why
    assert loop._cli_stderr[-1].endswith("ANTHROPIC_API_KEY is set")

    class _DyingClient:
        def __init__(self, options: Any) -> None:
            # what the CLI writes before exiting reaches the loop through the stderr callback
            options.stderr("Error: Invalid API key. Please run /login")

        async def __aenter__(self) -> Any:
            raise ProcessError(
                "Command failed with exit code 1",
                exit_code=1,
                stderr="Check stderr output for details",
            )

        async def __aexit__(self, *args: Any) -> None:
            return None

    monkeypatch.setattr(claude_sdk, "ClaudeSDKClient", _DyingClient)
    with pytest.raises(AgentRunFailed) as failed:
        asyncio.run(loop.run(inputs={}))

    said = str(failed.value)
    assert "exit code: 1" in said
    assert "Invalid API key" in said
