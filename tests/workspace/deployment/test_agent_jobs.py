"""Tests for `run.agent`, agent job definitions, and the agent launcher."""

import json as pyjson
import os
import sys
from contextlib import contextmanager
from functools import partial
from typing import Any, AsyncIterator, ClassVar, Dict, Iterator, List, Optional, Tuple, cast

import pytest
from pydantic_ai.messages import ModelMessage
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, FunctionModel

import dlt

from dlt.common.configuration import plugins
from dlt.common.configuration.container import Container
from dlt.common.configuration.plugins import PluginContext
from dlt.common.libs.pydantic import BaseModel
from dlt.common.typing import TypedDict

from dlt._workspace.deployment.agent.exceptions import InvalidAgentSpec
from dlt._workspace.deployment.agent.loop import AgentLoop
from dlt._workspace.deployment.agent.loops.pydantic_ai import PydanticAILoop
from dlt._workspace.deployment.agent.typing import TAgentJobResult, TAgentLimits, TAgentSpec
from dlt._workspace.deployment.decorators import AgentJobFactory, agent
from dlt._workspace.deployment.exceptions import (
    InvalidJobName,
    InvalidJobSchema,
    JobAbortedException,
)
from dlt._workspace.deployment.launchers import (
    DEFAULT_AGENT_LOOP,
    LAUNCHER_AGENT,
    agent_loop_group,
)
from dlt._workspace.deployment.launchers.agent import run as agent_run
from dlt._workspace.deployment.launchers.job import run_and_print_result
from dlt._workspace.deployment.manifest import (
    manifest_from_module,
    validate_job_definition,
    validate_manifest,
)
from dlt._workspace.deployment.typing import (
    TExecuteSpec,
    TJobRef,
    TRuntimeEntryPoint,
    TWorkspaceAccess,
)

from tests.workspace.utils import beacon as beacon, drain_beacon, importable_workspace


MOCK_LOOP = "mock-loop"


def agent_workspace() -> Any:
    """The agent workspace, which registers its mock loop when a job module imports it."""
    return importable_workspace(
        "agent_workspace", "__deployment__", "agent_jobs", "agent_batch_jobs", "mock_loop"
    )


def test_agent_decorator_dual_use() -> None:
    """All three call shapes produce an AgentJobFactory."""

    @agent
    def bare(run_context: Any = None) -> Dict[str, Any]:
        return {}

    @agent(loop=MOCK_LOOP, model="haiku", identity="ignored")
    def with_parens(run_context: Any = None) -> Dict[str, Any]:
        return {}

    declared = agent("dlthub-platform:job-inspector", loop=MOCK_LOOP)

    for factory in (bare, with_parens, declared):
        assert isinstance(factory, AgentJobFactory)
        assert factory.launcher == LAUNCHER_AGENT

    assert bare.loop == DEFAULT_AGENT_LOOP
    assert (bare.has_function, with_parens.has_function, declared.has_function) == (
        True,
        True,
        False,
    )
    # the name comes off the function, or off the agent ref
    assert (bare.name, with_parens.name, declared.name) == ("bare", "with_parens", "job_inspector")


def test_identity_is_accepted_and_not_stored() -> None:
    with agent_workspace():
        import agent_jobs  # type: ignore[import-not-found] # noqa: F401

        inspector = agent("dlthub-platform:job-inspector", identity="crash_inspector")
        inspector.bind_module_attr("agent_jobs", "inspector")
        assert "identity" not in inspector.to_job_definition()
    assert not hasattr(inspector, "identity")


@pytest.mark.parametrize(
    "ref,expected",
    [
        ("dlthub-platform:job-inspector", "job_inspector"),
        ("dq-sentinel", "dq_sentinel"),
        ("toolkit:multi-word-agent", "multi_word_agent"),
    ],
    ids=["toolkit-ref", "bare-name", "multi-word"],
)
def test_job_name_derived_from_agent_ref(ref: str, expected: str) -> None:
    assert agent(ref).name == expected


def test_non_identifier_agent_ref_is_rejected() -> None:
    with pytest.raises(InvalidJobName):
        agent("toolkit:9lives")


def test_declared_agent_job_definition() -> None:
    with agent_workspace():
        manifest, _ = manifest_from_module("__deployment__")
    jobs = {j["job_ref"]: j for j in manifest["jobs"]}
    definition = jobs[TJobRef("jobs.__deployment__.job_inspector")]

    entry = definition["entry_point"]
    assert entry["launcher"] == LAUNCHER_AGENT
    # the declaring module and the attribute name are stamped, so `function` is never None
    assert entry["function"] == "inspector"
    assert entry["job_type"] == "batch"
    assert definition["expose"]["category"] == "background_agent"
    # the loop reaches the runtime as the group that installs it
    assert agent_loop_group(MOCK_LOOP) in definition["require"]["dependency_groups"]
    # a requirement the user declared survives alongside it
    assert definition["require"]["timezone"] == "Europe/Berlin"
    # a job without a function takes its description from the agent definition
    assert definition["description"] == "Inspects a failed job run and reports a diagnosis."
    # the first entity-typed input tells the UI which entity's menu offers this job, and where
    # the chosen entity goes
    assert definition["expose"]["object_input"] == {
        "entity_type": "job-runs",
        "input": "jobs.__deployment__.job_inspector.failed_run_id",
    }
    assert definition["execute"] == {"concurrency": None}


def test_an_agent_job_declares_what_can_be_injected() -> None:
    """Every agent job holds to the rule: `inputs` is `config_keys`, typed."""
    with agent_workspace():
        manifest, _ = manifest_from_module("__deployment__")
        import agent_jobs

        agent_jobs.inspect_crash.bind_module_attr("agent_jobs", "inspect_crash")
        driver = agent_jobs.inspect_crash.to_job_definition()

    for job_def in manifest["jobs"]:
        declared = set((job_def.get("inputs") or {}).get("properties") or {})
        assert declared == set(job_def.get("config_keys") or []), job_def["job_ref"]

    # a function driving a referenced agent takes none of its inputs, so the job offers none:
    # its `AGENT.md` still declares them for the prompt, and dlt warns that nothing passes them
    assert "config_keys" not in driver
    assert "inputs" not in driver
    assert set(driver["output"]["properties"]) >= {"status", "summary"}
    assert driver["execute"] == {"concurrency": None}


def test_agent_block_and_config_keys_reach_the_manifest() -> None:
    """The declaration is read when the job definition is built, not when the loop runs."""
    with agent_workspace():
        import agent_jobs

        agent_jobs.inspector.bind_module_attr("agent_jobs", "inspector")
        job_def = agent_jobs.inspector.to_job_definition()

    agent_definition = job_def["agent"]
    assert agent_definition["name"] == "job-inspector"
    assert agent_definition["agent_file"].endswith(os.path.join("job-inspector", "AGENT.md"))
    assert agent_definition["instructions"].startswith("I expect incremental")
    assert "defaults" not in agent_definition
    assert "system_prompt" not in agent_definition
    # what the job may touch belongs to the job, not to the agent it runs
    assert job_def["access"] == {
        "local": ["all"],
        "data": ["read"],
        "context": ["read"],
    }
    assert "access" not in agent_definition
    # the declared inputs are the job's configuration, and the job declares them
    assert set(job_def["config_keys"]) == {"failed_run_id", "failed_job_ref"}
    assert set(job_def["inputs"]["properties"]) == {"failed_run_id", "failed_job_ref"}
    assert set(job_def["output"]["properties"]) >= {"status", "summary"}
    assert "inputs" not in agent_definition
    assert "output" not in agent_definition


def test_manifest_with_agents_validates() -> None:
    with agent_workspace():
        manifest, _ = manifest_from_module("__deployment__")
    result = validate_manifest(manifest)
    assert result.is_valid, result.errors
    assert result.unresolved_triggers == {}


def test_only_an_agent_job_declares_access() -> None:
    """`access` is a job field, but nothing except an agent fills it yet."""
    with agent_workspace():
        manifest, _ = manifest_from_module("__deployment__")
    jobs = {j["job_ref"]: j for j in manifest["jobs"]}

    assert jobs[TJobRef("jobs.__deployment__.job_inspector")]["access"] == {
        "local": ["all"],
        "data": ["read"],
        "context": ["read"],
    }
    assert "access" not in jobs[TJobRef("jobs.agent_batch_jobs.transform")]


def test_watcher_selector_expands_to_batch_jobs_only() -> None:
    with agent_workspace():
        manifest, _ = manifest_from_module("__deployment__")
    jobs = {j["job_ref"]: j for j in manifest["jobs"]}
    triggers = jobs[TJobRef("jobs.__deployment__.watcher")]["triggers"]
    # every batch job is a target, including agent jobs that are not themselves watchers
    assert triggers == [
        "job.fail:jobs.agent_batch_jobs.daily_ingest",
        "job.fail:jobs.agent_batch_jobs.transform",
        "job.fail:jobs.agent_jobs.inspect_crash",
        "job.fail:jobs.__deployment__.job_inspector",
        "job.fail:jobs.__deployment__.tag_watcher",
    ]
    # a watcher never targets itself, so a failure cannot re-trigger the run that reported it
    assert "job.fail:jobs.__deployment__.watcher" not in triggers
    # the same grammar selects by tag: only the job carrying it
    assert jobs[TJobRef("jobs.__deployment__.tag_watcher")]["triggers"] == [
        "job.fail:jobs.agent_batch_jobs.daily_ingest"
    ]
    # nor the interactive dashboard, which has no batch completion to watch
    assert not any("dashboard" in t for t in triggers)


def _entry(function: str) -> TRuntimeEntryPoint:
    """Entry point as the manifest records it: the deployment module and the attribute."""
    return {
        "module": "__deployment__",
        "function": function,
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef(f"jobs.__deployment__.{function}"),
    }


def test_launcher_runs_a_declared_agent(beacon: List[Tuple[str, str]]) -> None:
    with agent_workspace() as ctx:
        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        output = agent_run(_entry("inspector"), run_id="r-1", trigger="job.fail:jobs.b.ingest")
        drain_beacon()

    assert output["type"] == "job.background_agent.dlthub-platform:job-inspector"
    assert output["status"] == "succeeded"
    assert output["trace"]["turn_count"] == 3

    body = pyjson.loads(beacon[-1][1])
    assert "run_id" not in body
    assert body["job_ref"] == "jobs.__deployment__.job_inspector"


def test_inputs_validator_extends_the_inputs() -> None:
    with agent_workspace():
        output = agent_run(_entry("inspector"), run_id="r-2", trigger="manual:")
    # the validator supplied a job ref the trigger did not carry
    assert output["trace"]["inputs"]["failed_job_ref"] == "jobs.batch.ingest"
    # the loop handle never reaches the trace: it is neither useful nor serializable there
    assert "ai_loop" not in output["trace"]["inputs"]["run_context"]


def test_aborted_agent_raises_after_delivering(
    beacon: List[Tuple[str, str]], capsys: pytest.CaptureFixture[str]
) -> None:
    with agent_workspace() as ctx:
        import mock_loop  # type: ignore[import-not-found]

        mock_loop.MockLoop.outcome = {
            "status": "aborted",
            "summary": "no failed run id could be resolved",
        }
        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        with pytest.raises(JobAbortedException, match="no failed run id") as exc:
            run_and_print_result(
                partial(agent_run, _entry("inspector"), run_id="r-3", trigger="manual:")
            )

    # the exception carries the result as delivered, and the launcher printed it before raising
    assert exc.value.result["status"] == "aborted"  # type: ignore[typeddict-item]
    assert exc.value.result["job_ref"] == "jobs.__deployment__.job_inspector"
    out = capsys.readouterr().out
    assert "Result  [job.background_agent.dlthub-platform:job-inspector]" in out
    assert "status:     ❗ aborted" in out
    assert "summary:    no failed run id could be resolved" in out
    # an abort ends the process, so the trace must already be on the wire
    assert len(beacon) == 1
    body = pyjson.loads(beacon[0][1])
    assert body["status"] == "aborted"
    assert "trace" in body


@pytest.mark.parametrize(
    "function,status", [("cached", "succeeded"), ("gives_up", "aborted")], ids=["cached", "aborts"]
)
def test_an_agent_may_return_without_calling_its_loop(function: str, status: str) -> None:
    """The result stands, and the trace is that of a run with no turns."""
    ep: TRuntimeEntryPoint = {
        "module": "agent_jobs",
        "function": function,
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef(f"jobs.agent_jobs.{function}"),
    }
    with agent_workspace():
        if status == "aborted":
            with pytest.raises(JobAbortedException, match="nothing to inspect") as exc:
                agent_run(ep, run_id="r-4", trigger="manual:")
            result = cast(TAgentJobResult, exc.value.result)
        else:
            result = agent_run(ep, run_id="r-4", trigger="manual:")

    assert result["status"] == status
    trace = result["trace"]
    assert (trace["turn_count"], trace["total_tokens"]) == (0, 0)
    assert trace["loop_type"] == MOCK_LOOP and trace["model"]
    assert "ai_loop" not in trace["inputs"]["run_context"]


def _code_entry(function: str, failed_run_id: Optional[str] = None) -> TRuntimeEntryPoint:
    ep: TRuntimeEntryPoint = {
        "module": "agent_code_jobs",
        "function": function,
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef(f"jobs.agent_code_jobs.{function}"),
    }
    if failed_run_id:
        ep["run_args"] = {"failed_run_id": failed_run_id}  # type: ignore[typeddict-unknown-key]
    return ep


def agent_code_workspace() -> Any:
    return importable_workspace(
        "agent_workspace", "agent_code_jobs", "mock_loop", "checked-inspector", "broken-code"
    )


def test_agent_code_runs_before_and_after_the_loop() -> None:
    """`agent.py` next to `AGENT.md` extends the inputs and rewrites the output, unasked."""
    with agent_code_workspace():
        output = agent_run(_code_entry("checked"), run_id="r-1", trigger="manual:")
        # a validator returning nothing keeps what it was given
        kept = agent_run(_code_entry("checked", "keep"), run_id="r-2", trigger="manual:")

    # `validate_input` filled the run id through its sibling `helpers.py`, and the model saw it
    assert output["trace"]["inputs"]["failed_run_id"] == "r-prepared"
    assert "You inspect run 'r-prepared'" in output["result"]["ran"]["system_prompt"]
    # `validate_output` read what `validate_input` prepared, through the module's own state
    assert output["result"]["checked"] == "checked by agent.py"
    assert output["result"]["prepared_for"] == "r-prepared"

    assert kept["trace"]["inputs"]["failed_run_id"] == "keep"
    assert "checked" not in kept["result"]
    assert kept["summary"] == "mock run"


def test_agent_code_runs_before_the_job_validators() -> None:
    """A job's own validators refine what the agent's code returned."""
    with agent_code_workspace():
        import agent_code_jobs  # type: ignore[import-not-found]

        agent_code_jobs.SEEN.clear()
        output = agent_run(_code_entry("checked_twice"), run_id="r-3", trigger="manual:")

    seen_inputs, seen_output = agent_code_jobs.SEEN
    assert seen_inputs["inputs"]["failed_run_id"] == "r-prepared"
    assert seen_output["output"]["checked"] == "checked by agent.py"
    assert output["trace"]["inputs"]["failed_run_id"] == "r-prepared+job"
    assert output["summary"] == "refined by the job"


def test_agent_code_that_raises_fails_the_job() -> None:
    with agent_code_workspace():
        with pytest.raises(ValueError, match="could not read the run"):
            agent_run(_code_entry("checked", "boom"), run_id="r-4", trigger="manual:")


def test_agent_code_may_end_the_run_before_the_loop() -> None:
    """`JobAbortedException` from `validate_input` delivers an aborted result; no model call."""
    with agent_code_workspace():
        # the result's summary names the abort, the reason the code gave stays chained
        with pytest.raises(JobAbortedException, match="no failed run found") as exc:
            agent_run(_code_entry("checked", "nothing"), run_id="r-5", trigger="manual:")

    assert "nothing to inspect" in str(exc.value.__context__)
    result = cast(TAgentJobResult, exc.value.result)
    assert (result["status"], result["summary"]) == ("aborted", "no failed run found")
    assert result["trace"]["turn_count"] == 0
    assert "checked" not in result["result"]


def test_agent_code_is_not_imported_for_the_manifest() -> None:
    """Deploying never runs agent code: the manifest only reads `AGENT.md`."""
    with agent_code_workspace():
        import agent_code_jobs

        agent_code_jobs.broken.bind_module_attr("agent_code_jobs", "broken")
        assert agent_code_jobs.broken.to_job_definition()["agent"]["name"] == "broken-code"
        # the run is what imports it
        with pytest.raises(RuntimeError, match="broken-code was imported"):
            agent_run(_code_entry("broken"), run_id="r-6", trigger="manual:")


def test_agent_launcher_shares_the_job_launcher_setup() -> None:
    """Interval injection and signal interception come from the job launcher, not a copy of it."""
    import signal

    from dlt.common.runtime import signals

    ep: TRuntimeEntryPoint = {
        "module": "agent_jobs",
        "function": "interval_aware",
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef("jobs.agent_jobs.interval_aware"),
        "interval_start": "2024-01-15T00:00:00Z",
        "interval_end": "2024-01-16T00:00:00Z",
        "intercept_signals": False,
    }
    with agent_workspace():
        result = agent_run(ep, run_id="iv-1", trigger="schedule:0 0 * * *")
        assert result["ctx_start"].startswith("2024-01-15")
        # dlt.current.interval() needs TimeIntervalContext, which only the job launcher injects
        assert result["current_start"].startswith("2024-01-15")
        # `intercept_signals=False` was ignored before the launchers were shared
        assert result["sigint_handler"] is not signals._signal_receiver

        ep["intercept_signals"] = True
        intercepted = agent_run(ep, run_id="iv-2", trigger="schedule:0 0 * * *")
        assert intercepted["sigint_handler"] is signals._signal_receiver
        assert signal.getsignal(signal.SIGINT) is not signals._signal_receiver


@pytest.mark.parametrize(
    "argument,value",
    [
        ("inputs_validator", lambda inputs: inputs),
        ("outputs_validator", lambda output: output),
    ],
    ids=["inputs_validator", "outputs_validator"],
)
def test_function_form_rejects_declared_agent_arguments(argument: str, value: Any) -> None:
    """Only the launcher-driven form accepts these. On a function, dlt ignores them."""
    with pytest.raises(TypeError, match=argument):

        @agent(**{argument: value})
        def driver(run_context: Any = None) -> Dict[str, Any]:
            return {}

    # the same arguments are accepted when an agent is named
    assert agent("toolkit:inspector", **{argument: value}) is not None


@pytest.mark.parametrize(
    "argument,value",
    [
        ("interval", {"start": "2024-01-01"}),
        ("freshness", "is_fresh"),
        ("allow_external_schedulers", True),
        ("refresh", "always"),
    ],
    ids=["interval", "freshness", "allow-external-schedulers", "refresh"],
)
def test_agent_does_not_take_interval_scheduling_arguments(argument: str, value: Any) -> None:
    with pytest.raises(TypeError, match=argument):
        agent("toolkit:inspector", **{argument: value})


MINIMAL_AGENT: TAgentSpec = {
    "name": "inline-agent",
    "description": "d",
    "access": {},
    "inputs": {"type": "object", "properties": {"why": {"type": "string"}}},
    "output": {"type": "object", "properties": {"status": {}, "summary": {}}},
    "system_prompt": "Explain {{ why }}.",
}


@pytest.mark.parametrize(
    "given,match",
    [
        ("", "Empty agent definition reference"),
        ("  ", "Empty agent definition reference"),
        ({}, "has no 'name'"),
        ({"description": "no name"}, "has no 'name'"),
    ],
    ids=["empty-ref", "blank-ref", "empty-spec", "nameless-spec"],
)
def test_agent_without_an_agent_is_refused(given: Any, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        agent(given, loop=MOCK_LOOP)
    with pytest.raises(ValueError, match=match):

        @agent(agent=given, loop=MOCK_LOOP)
        def driver(run_context: Any = None) -> Dict[str, Any]:
            return {}


def test_agent_is_named_by_reference_or_given_in_full() -> None:
    """Both forms take either a `<toolkit>:<agent>` reference or a `TAgentSpec`."""
    by_ref = agent("dlthub-platform:job-inspector", loop=MOCK_LOOP)
    in_full = agent(MINIMAL_AGENT, loop=MOCK_LOOP)

    @agent(agent="dlthub-platform:job-inspector", loop=MOCK_LOOP)
    def driver(run_context: Any = None) -> Dict[str, Any]:
        return {}

    assert (by_ref.agent_ref, by_ref.agent_spec) == ("dlthub-platform:job-inspector", None)
    assert in_full.agent_spec is MINIMAL_AGENT
    # the job name comes off the agent name either way
    assert (by_ref.name, in_full.name) == ("job_inspector", "inline_agent")
    # a decorated function keeps its own body, and now has an agent to build a loop from
    assert driver.has_function
    assert driver.agent_ref == "dlthub-platform:job-inspector"


def test_function_job_inputs_keep_the_entity_types_of_the_agent_definition() -> None:
    """Parameters without annotations take type, description and entity type from `AGENT.md`."""

    @agent(agent="dlthub-platform:job-inspector", loop=MOCK_LOOP)
    def inspect_run(failed_run_id=dlt.config.value, run_context: Any = None) -> Dict[str, Any]:
        return {}

    with agent_workspace():
        inputs = inspect_run.to_job_definition()["inputs"]

    assert inputs["properties"]["failed_run_id"]["type"] == "string"
    assert inputs["properties"]["failed_run_id"]["entity_type"] == "job-runs"
    assert (
        inputs["properties"]["failed_run_id"]["description"]
        == "explicit run id of the job that failed"
    )
    # the function takes no `failed_job_ref`, so the job has no such input
    assert "failed_job_ref" not in inputs["properties"]


def test_an_agent_declaring_no_access_still_states_it() -> None:
    """`{}` is an answer: the job says it may touch nothing, rather than saying nothing."""
    job = agent(MINIMAL_AGENT, loop=MOCK_LOOP, name="minimal")
    job.bind_module_attr(__name__, "minimal")
    with agent_workspace():
        assert job.to_job_definition()["access"] == {}


class _ExitCode(TypedDict):
    exit_code: int


class _ExitCodeModel(BaseModel):
    exit_code: int


@pytest.mark.parametrize(
    "output",
    [
        {"type": "object", "properties": {"exit_code": {"type": "integer"}}},
        _ExitCode,
        _ExitCodeModel,
    ],
    ids=["schema", "typeddict", "pydantic"],
)
def test_the_agent_argument_can_carry_only_the_output(output: Any) -> None:
    """The function stays the agent: its docstring the prompt, its parameters the inputs.

    `agent=` then adds what the signature cannot say, here the output as a schema or a model.
    """

    @agent(agent=cast(TAgentSpec, {"name": "reporter", "output": output}), loop=MOCK_LOOP)
    async def exit_code(run_context: Any = None, depth: int = 2) -> None:
        """Report the exit code. Look {{ depth }} runs back."""

    with agent_workspace():
        job_def = exit_code.to_job_definition()

    assert set(job_def["output"]["properties"]) == {"exit_code", "status", "summary"}
    assert job_def["output"]["properties"]["exit_code"]["type"] == "integer"
    assert set(job_def["inputs"]["properties"]) == {"depth"}
    assert exit_code.agent_spec["system_prompt"].startswith("Report the exit code.")
    if isinstance(output, dict):
        # the dict handed to the decorator is left as it was
        assert "status" not in output["properties"]


def test_an_agent_without_a_description_leaves_the_job_without_one() -> None:
    """A job without a function takes its description from the agent definition, if it has one."""
    spec = {key: value for key, value in MINIMAL_AGENT.items() if key != "description"}
    job = agent(cast(TAgentSpec, spec), loop=MOCK_LOOP, name="quiet")
    job.bind_module_attr(__name__, "quiet")
    with agent_workspace():
        job_def = job.to_job_definition()

    assert "description" not in job_def
    assert "description" not in job_def["agent"]


def test_unknown_entity_type_fails_at_manifest_time() -> None:
    """A typo in `entity_type` is refused when the manifest is built, not when a run finishes."""
    spec = dict(MINIMAL_AGENT)
    spec["inputs"] = {
        "type": "object",
        "properties": {"why": {"type": "string", "entity_type": "pipline"}},
    }
    job = agent(cast(TAgentSpec, spec), loop=MOCK_LOOP, name="typo")
    job.bind_module_attr(__name__, "typo")
    with agent_workspace():
        with pytest.raises(InvalidJobSchema, match="why: entity_type 'pipline'"):
            job.to_job_definition()


def test_legacy_job_run_entity_type_reaches_the_manifest_unchanged() -> None:
    """Older backends read `job-run`, so the manifest keeps what the agent declared."""
    inputs: Dict[str, Any] = {
        "type": "object",
        "properties": {"run_id": {"type": "string", "entity_type": "job-run"}},
    }
    job = agent(cast(TAgentSpec, {**MINIMAL_AGENT, "inputs": inputs}), loop=MOCK_LOOP, name="old")
    job.bind_module_attr(__name__, "old")
    with agent_workspace():
        job_def = job.to_job_definition()
        manifest, _ = manifest_from_module("__deployment__")

    assert job_def["inputs"]["properties"]["run_id"]["entity_type"] == "job-run"
    assert job_def["expose"]["object_input"]["entity_type"] == "job-run"
    manifest["jobs"].append(job_def)
    result = validate_manifest(manifest)
    assert result.is_valid, result.errors


def test_agent_given_positionally_rejects_the_keyword() -> None:
    """The overloads already refuse this; the runtime says so too."""
    with pytest.raises(TypeError, match="positionally"):
        agent("dlthub-platform:job-inspector", agent=MINIMAL_AGENT)  # type: ignore[call-overload]


def _agent_with_defaults(**defaults: Any) -> TAgentSpec:
    """`MINIMAL_AGENT` with a `defaults` block."""
    return cast(TAgentSpec, {**MINIMAL_AGENT, "name": "fan-out", "defaults": defaults})


def test_agent_defaults_fill_trigger_and_execute() -> None:
    job = agent(
        _agent_with_defaults(trigger=["0 7 * * *"], execute={"concurrency": None, "timeout": 600}),
        loop=MOCK_LOOP,
    )
    job.bind_module_attr(__name__, "fan_out")
    with agent_workspace():
        job_def = job.to_job_definition()
        # built again, the definition is the same
        assert job.to_job_definition() == job_def

    assert job_def["triggers"] == ["schedule:0 7 * * *"]
    # `null` lifts the cap dlt gives every job
    assert job_def["execute"] == {"concurrency": None, "timeout": {"timeout": 600.0}}
    validate_job_definition(job_def, validate_dict=True, raise_on_error=True)


def test_an_agent_without_defaults_leaves_the_job_defaults_in_place() -> None:
    job = agent(MINIMAL_AGENT, loop=MOCK_LOOP, name="plain")
    job.bind_module_attr(__name__, "plain")
    with agent_workspace():
        job_def = job.to_job_definition()

    assert job_def["triggers"] == []
    assert job_def["execute"] == {"concurrency": 1}


@pytest.mark.parametrize("execute", [{}, None], ids=["empty", "null"])
def test_an_agent_saying_nothing_about_execute_leaves_the_job_defaults(execute: Any) -> None:
    job = agent(_agent_with_defaults(execute=execute), loop=MOCK_LOOP)
    job.bind_module_attr(__name__, "empty_execute")
    with agent_workspace():
        assert job.to_job_definition()["execute"] == {"concurrency": 1}


@pytest.mark.parametrize(
    "defaults,reason",
    [
        ({"execute": {"concurrency": 0}}, "positive integer"),
        ({"execute": {"concurrency": "five"}}, "positive integer"),
        ({"execute": {"concurrency": True}}, "positive integer"),
        ({"execute": {"timeout": "soon"}}, "does not parse"),
        ({"execute": {"parallelism": 2}}, "takes timeout, concurrency"),
        ({"execute": {"intercept_signals": False}}, "takes timeout, concurrency"),
        ({"execute": "none"}, "defaults.execute must be a mapping"),
        ("nope", "defaults must be a mapping"),
    ],
    ids=[
        "zero",
        "string",
        "bool",
        "bad-timeout",
        "unknown-key",
        "signals",
        "execute-not-a-mapping",
        "defaults-not-a-mapping",
    ],
)
def test_bad_defaults_fail_at_manifest_time(defaults: Any, reason: str) -> None:
    """An agent given in full goes through the same checks as an `AGENT.md`."""
    job = agent(cast(TAgentSpec, {**MINIMAL_AGENT, "defaults": defaults}), loop=MOCK_LOOP)
    job.bind_module_attr(__name__, "broken")
    with agent_workspace():
        with pytest.raises(InvalidAgentSpec, match=reason):
            job.to_job_definition()


AGENT_DEFAULTS: Dict[str, Any] = {"trigger": ["job.fail:*"], "execute": {"concurrency": 4}}


def _agent_job(
    form: str, trigger: Any = None, execute: Optional[TExecuteSpec] = None
) -> "AgentJobFactory[Any, Any]":
    spec = _agent_with_defaults(**AGENT_DEFAULTS)
    factory: AgentJobFactory[Any, Any]
    if form == "declared":
        factory = agent(spec, loop=MOCK_LOOP, trigger=trigger, execute=execute)
    else:

        @agent(agent=spec, loop=MOCK_LOOP, trigger=trigger, execute=execute)
        def driver(run_context: Any = None) -> Dict[str, Any]:
            return {}

        factory = driver
    factory.bind_module_attr(__name__, f"from_agent_{form}")
    return factory


@pytest.mark.parametrize("form", ["declared", "function"])
@pytest.mark.parametrize(
    "trigger,execute,expected_triggers,expected_execute",
    [
        (None, None, ["job.fail:*"], {"concurrency": 4}),
        ([], None, [], {"concurrency": 4}),
        ("0 9 * * *", None, ["schedule:0 9 * * *"], {"concurrency": 4}),
        (None, {}, ["job.fail:*"], {"concurrency": 4}),
        (
            None,
            {"timeout": {"timeout": 300.0}},
            ["job.fail:*"],
            {"concurrency": 4, "timeout": {"timeout": 300.0}},
        ),
        (None, {"concurrency": None}, ["job.fail:*"], {"concurrency": None}),
        ("0 9 * * *", {"concurrency": 2}, ["schedule:0 9 * * *"], {"concurrency": 2}),
    ],
    ids=[
        "nothing-said",
        "no-triggers",
        "other-trigger",
        "empty-execute",
        "timeout-added",
        "cap-lifted",
        "both-replaced",
    ],
)
def test_decorator_overrides_agent_defaults_key_by_key(
    form: str,
    trigger: Any,
    execute: Optional[TExecuteSpec],
    expected_triggers: List[str],
    expected_execute: Dict[str, Any],
) -> None:
    """What the decorator leaves out, the agent's `defaults` fill. `[]` and `None` are not left out."""
    with agent_workspace():
        job_def = _agent_job(form, trigger=trigger, execute=execute).to_job_definition()
    assert job_def["triggers"] == expected_triggers
    assert job_def["execute"] == expected_execute
    validate_job_definition(job_def, validate_dict=True, raise_on_error=True)


async def _triage_model(messages: List[ModelMessage], info: AgentInfo) -> AsyncIterator[Any]:
    """Answers from the error in the rendered system prompt, in place of a model."""
    category = "config" if "credentials" in info.instructions else "infra"
    answer = {"status": "succeeded", "summary": f"looks like {category}", "category": category}
    yield {0: DeltaToolCall(name=info.output_tools[0].name, json_args=pyjson.dumps(answer))}


def test_agent_job_runs_an_agent_definition_per_input_and_reports_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A decorated agent job awaits a `run.agent` job once per error, then returns one output.

    Each run goes through the pydantic-ai loop and the `agent.py` of the agent definition.
    """
    monkeypatch.setattr(
        PydanticAILoop, "_build_model", lambda self: FunctionModel(stream_function=_triage_model)
    )
    errors = ["  missing credentials for postgres ", "warehouse timed out", "   "]
    ep: TRuntimeEntryPoint = {
        "module": "agent_triage_jobs",
        "function": "triage_report",
        "job_type": "batch",
        "launcher": LAUNCHER_AGENT,
        "job_ref": TJobRef("jobs.agent_triage_jobs.triage_report"),
        "run_args": {"errors": errors},  # type: ignore[typeddict-unknown-key]
    }
    with importable_workspace("agent_workspace", "agent_triage_jobs"):
        import agent_triage_jobs  # type: ignore[import-not-found]

        output = agent_run(ep, run_id="r-1", trigger="manual:")
        last_triage = agent_triage_jobs.triage.last_job_result

    assert output["type"] == "job.background_agent.triage-report"
    assert output["status"] == "succeeded"
    report = output["result"]
    assert report["by_category"] == {"config": 1, "infra": 1}
    # `validate_output` of agent.py set the owner of each category
    assert report["owners"] == ["data-eng", "platform"]
    # `validate_input` of agent.py aborted the blank error before the loop started
    assert report["skipped"] == 1
    # the report never called its own loop
    assert output["trace"]["turn_count"] == 0

    # each call keeps its own job result: the last one is the abort
    assert last_triage["status"] == "aborted"
    assert last_triage["summary"] == "empty error message"
