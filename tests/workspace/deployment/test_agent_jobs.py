"""Tests for `run.agent`, agent job definitions, and the agent launcher."""

import json as pyjson
import os
import sys
from contextlib import contextmanager
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest

from dlt.common.configuration import plugins
from dlt.common.configuration.container import Container
from dlt.common.configuration.plugins import PluginContext
from dlt.pipeline import platform

from dlt._workspace.deployment.agent.loop import AgentLoop
from dlt._workspace.deployment.agent.typing import TAgentLimits, TAgentSpec
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
from dlt._workspace.deployment.manifest import manifest_from_module, validate_manifest
from dlt._workspace.deployment.typing import TWorkspaceAccess, TJobRef, TRuntimeEntryPoint

from tests.workspace.utils import importable_workspace


MOCK_LOOP = "mock-loop"


def agent_workspace() -> Any:
    """The agent workspace, which registers its mock loop when a job module imports it."""
    return importable_workspace(
        "agent_workspace", "__deployment__", "agent_jobs", "agent_batch_jobs", "mock_loop"
    )


@pytest.fixture
def beacon() -> Iterator[List[Tuple[str, str]]]:
    sent: List[Tuple[str, str]] = []

    def _put(url: str, data: str) -> Any:
        sent.append((url, data))
        return MagicMock(status_code=200)

    platform._THREAD_POOL = None
    platform.init_platform_tracker()
    with patch.object(platform, "requests") as requests:
        requests.put.side_effect = _put
        yield sent
        _drain()
    platform._THREAD_POOL = None


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
    assert (bare.is_declared, with_parens.is_declared, declared.is_declared) == (
        False,
        False,
        True,
    )
    # the name comes off the function, or off the agent ref
    assert (bare.name, with_parens.name, declared.name) == ("bare", "with_parens", "job_inspector")


def test_identity_is_accepted_and_not_stored() -> None:
    with agent_workspace():
        import agent_jobs  # type: ignore[import-not-found] # noqa: F401

        inspector = agent("dlthub-platform:job-inspector", identity="crash_inspector")
        inspector.declare("agent_jobs", "inspector")
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
    # a declared job has no function, so the agent describes it
    assert definition["description"] == "Inspects a failed job run and reports a diagnosis."
    # the first entity-typed input tells the UI which entity's menu offers this job
    assert definition["expose"]["object_type"] == "job-run"


def test_an_agent_job_declares_what_can_be_injected() -> None:
    """Every agent job holds to the rule: `inputs` is `config_keys`, typed."""
    with agent_workspace():
        manifest, _ = manifest_from_module("__deployment__")
        import agent_jobs

        agent_jobs.inspect_crash.declare("agent_jobs", "inspect_crash")
        driver = agent_jobs.inspect_crash.to_job_definition()

    for job_def in manifest["jobs"]:
        declared = set((job_def.get("inputs") or {}).get("properties") or {})
        assert declared == set(job_def.get("config_keys") or []), job_def["job_ref"]

    # a function driving a referenced agent takes none of its inputs, so the job offers none:
    # its `AGENT.md` still declares them for the prompt, and dlt warns that nothing passes them
    assert "config_keys" not in driver
    assert "inputs" not in driver
    assert set(driver["output"]["properties"]) >= {"status", "summary"}


def test_agent_block_and_config_keys_reach_the_manifest() -> None:
    """The declaration is read when the job definition is built, not when the loop runs."""
    with agent_workspace():
        import agent_jobs

        agent_jobs.inspector.declare("agent_jobs", "inspector")
        job_def = agent_jobs.inspector.to_job_definition()

    agent_definition = job_def["agent"]
    assert agent_definition["name"] == "job-inspector"
    assert agent_definition["agent_file"].endswith(os.path.join("job-inspector", "AGENT.md"))
    assert agent_definition["instructions"].startswith("I expect incremental")
    assert "defaults" not in agent_definition
    assert "system_prompt" not in agent_definition
    # what the job may touch belongs to the job, not to the agent it runs
    assert job_def["access"] == {
        "toolkits": True,
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
        "toolkits": True,
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


def _drain() -> None:
    assert platform._THREAD_POOL is not None
    platform._THREAD_POOL.thread_pool.shutdown(wait=True)


def test_launcher_runs_a_declared_agent(beacon: List[Tuple[str, str]]) -> None:
    with agent_workspace() as ctx:
        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        output = agent_run(_entry("inspector"), run_id="r-1", trigger="job.fail:jobs.b.ingest")
        _drain()

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


def test_aborted_agent_raises_after_delivering(beacon: List[Tuple[str, str]]) -> None:
    with agent_workspace() as ctx:
        import mock_loop  # type: ignore[import-not-found]

        mock_loop.MockLoop.outcome = {
            "status": "aborted",
            "summary": "no failed run id could be resolved",
        }
        ctx.runtime_config.dlthub_dsn = "https://beacon.example/token"
        with pytest.raises(JobAbortedException, match="no failed run id") as exc:
            agent_run(_entry("inspector"), run_id="r-3", trigger="manual:")

    assert exc.value.result["status"] == "aborted"  # type: ignore[typeddict-item]
    # an abort ends the process, so the trace must already be on the wire
    assert len(beacon) == 1
    body = pyjson.loads(beacon[0][1])
    assert body["status"] == "aborted"
    assert "trace" in body


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
    assert (driver.is_declared, driver.has_agent) == (False, True)


def test_an_agent_declaring_no_access_still_states_it() -> None:
    """`{}` is an answer: the job says it may touch nothing, rather than saying nothing."""
    job = agent(MINIMAL_AGENT, loop=MOCK_LOOP, name="minimal")
    job.declare(__name__, "minimal")
    with agent_workspace():
        assert job.to_job_definition()["access"] == {}


def test_an_agent_without_a_description_leaves_the_job_without_one() -> None:
    """A declared job takes its description from the agent, and an agent needs none."""
    spec = {key: value for key, value in MINIMAL_AGENT.items() if key != "description"}
    job = agent(cast(TAgentSpec, spec), loop=MOCK_LOOP, name="quiet")
    job.declare(__name__, "quiet")
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
    job.declare(__name__, "typo")
    with agent_workspace():
        with pytest.raises(InvalidJobSchema, match="why: entity_type 'pipline'"):
            job.to_job_definition()


def test_agent_given_positionally_rejects_the_keyword() -> None:
    """The overloads already refuse this; the runtime says so too."""
    with pytest.raises(TypeError, match="positionally"):
        agent("dlthub-platform:job-inspector", agent=MINIMAL_AGENT)  # type: ignore[call-overload]
